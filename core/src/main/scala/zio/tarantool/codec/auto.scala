package zio.tarantool.codec

import org.msgpack.value.{ArrayValue, Value}
import org.msgpack.value.impl.ImmutableNilValueImpl
import shapeless.{CNil, _}
import shapeless.labelled.{FieldType, field}
import zio.tarantool.TarantoolError.CodecError

import scala.util.Try

object auto extends LowPriorityInstances {
  implicit class TupleUpdateBuilder[A <: Product](val value: A) {
    def builder(implicit opsBuilder: TupleOpsBuilder[A]): TupleOpsBuilder[A] = opsBuilder
  }
}

private[tarantool] trait LowPriorityInstances extends LowestPriorityInstances {
  final implicit def genericFamilyEncoder[A, H <: Coproduct](implicit
    gen: LabelledGeneric.Aux[A, H],
    hEncoder: Lazy[TupleEncoder[H]],
    notOption: A <:!< Option[Z] forSome { type Z }
  ): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(v: A): Vector[Value] = hEncoder.value.encode(gen.to(v))

    override def decode(v: ArrayValue, idx: Int): A =
      gen.from(hEncoder.value.decode(v, idx))
  }

  final implicit val cnilEncoder: TupleEncoder[CNil] = new TupleEncoder[CNil] {
    override def encode(v: CNil): Vector[Value] = Vector(ImmutableNilValueImpl.get())

    override def decode(v: ArrayValue, idx: Int): CNil =
      throw new NotImplementedError("CNil")
  }

  final implicit def coproductEncoder[K <: Symbol, H, T <: Coproduct](implicit
    hEncoder: Lazy[TupleEncoder[H]],
    tEncoder: Lazy[TupleEncoder[T]]
  ): TupleEncoder[FieldType[K, H] :+: T] = new TupleEncoder[FieldType[K, H] :+: T] {
    override def encode(v: FieldType[K, H] :+: T): Vector[Value] =
      v match {
        case Inl(head) => hEncoder.value.encode(head)
        case Inr(tail) => tEncoder.value.encode(tail)
      }

    override def decode(v: ArrayValue, idx: Int): FieldType[K, H] :+: T =
      Try(hEncoder.value.decode(v, idx))
        .map(h => Inl(field[K](h)))
        .orElse(Try(tEncoder.value.decode(v, idx)).map(t => Inr(t)))
        .getOrElse(
          throw CodecError(
            new IllegalArgumentException("Unexpected error while decoding coproduct")
          )
        )
  }

  // todo: flat structure or nested?
  implicit def genericEncoder[A, H <: HList](implicit
    gen: LabelledGeneric.Aux[A, H],
    hEncoder: Lazy[TupleEncoder[H]]
  ): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(v: A): Vector[Value] = hEncoder.value.encode(gen.to(v))

    override def decode(v: ArrayValue, idx: Int): A =
      gen.from(hEncoder.value.decode(v, idx))
  }

  implicit val hnilEncoder: TupleEncoder[HNil] = new TupleEncoder[HNil] {
    override def encode(v: HNil): Vector[Value] = Vector.empty

    override def decode(v: ArrayValue, idx: Int): HNil = HNil
  }

  implicit def hlistEncoder[K <: Symbol, H, T <: HList](implicit
    hEncoder: Lazy[TupleEncoder[H]],
    tEncoder: Lazy[TupleEncoder[T]]
  ): TupleEncoder[FieldType[K, H] :: T] = new TupleEncoder[FieldType[K, H] :: T] {
    override def encode(v: FieldType[K, H] :: T): Vector[Value] = v match {
      case h :: t =>
        val head: Vector[Value] = hEncoder.value.encode(h)
        val tail: Vector[Value] = tEncoder.value.encode(t)

        head ++ tail
    }

    override def decode(v: ArrayValue, idx: Int): FieldType[K, H] :: T = {
      val head = hEncoder.value.decode(v, idx)
      val tail = tEncoder.value.decode(v, idx + 1)

      field[K](head) :: tail
    }
  }
}

private[tarantool] trait LowestPriorityInstances {
  implicit def genericOptionEncoder[A, H <: HList](implicit
    gen: LabelledGeneric.Aux[A, H],
    hEncoder: Lazy[TupleEncoder[Option[H]]]
  ): TupleEncoder[Option[A]] = new TupleEncoder[Option[A]] {
    override def encode(v: Option[A]): Vector[Value] = v match {
      case value @ Some(_) => hEncoder.value.encode(value.map(gen.to))
      case None            => Vector.empty
    }

    override def decode(v: ArrayValue, idx: Int): Option[A] =
      hEncoder.value.decode(v, idx).map(gen.from)
  }

  implicit val hnilOptionEncoder: TupleEncoder[Option[HNil]] = new TupleEncoder[Option[HNil]] {
    override def encode(v: Option[HNil]): Vector[Value] = Vector.empty

    override def decode(v: ArrayValue, idx: Int): Option[HNil] = Some(HNil)
  }

  implicit def hlistOptionEncoder1[K <: Symbol, H, T <: HList](implicit
    hEncoder: Lazy[TupleEncoder[Option[H]]],
    tEncoder: Lazy[TupleEncoder[Option[T]]],
    notOption: H <:!< Option[Z] forSome { type Z }
  ): TupleEncoder[Option[FieldType[K, H] :: T]] = new TupleEncoder[Option[FieldType[K, H] :: T]] {
    override def encode(v: Option[FieldType[K, H] :: T]): Vector[Value] = {
      def split[A](v: Option[H :: T])(f: (Option[H], Option[T]) => A): A = v.fold(f(None, None))({
        case h :: t => f(Some(h), Some(t))
      })

      split(v) { case (head, tail) =>
        val encodedHead: Vector[Value] = hEncoder.value.encode(head)
        val encodedTail: Vector[Value] = tEncoder.value.encode(tail)

        encodedHead ++ encodedTail
      }
    }

    override def decode(v: ArrayValue, idx: Int): Option[FieldType[K, H] :: T] = {
      val head = hEncoder.value.decode(v, idx)
      val tail = tEncoder.value.decode(v, idx + 1)

      head.flatMap(h => tail.map(t => field[K](h) :: t))
    }

  }

  implicit def hlistOptionEncoder2[K <: Symbol, H, T <: HList](implicit
    hEncoder: Lazy[TupleEncoder[Option[H]]],
    tEncoder: Lazy[TupleEncoder[Option[T]]]
  ): TupleEncoder[Option[FieldType[K, Option[H]] :: T]] =
    new TupleEncoder[Option[FieldType[K, Option[H]] :: T]] {
      override def encode(v: Option[FieldType[K, Option[H]] :: T]): Vector[Value] = {
        def split[A](v: Option[Option[H] :: T])(f: (Option[H], Option[T]) => A): A =
          v.fold(f(None, None))({ case h :: t => f(h, Some(t)) })

        split(v) { case (head, tail) =>
          val encodedHead: Vector[Value] = hEncoder.value.encode(head)
          val encodedTail: Vector[Value] = tEncoder.value.encode(tail)

          encodedHead ++ encodedTail
        }
      }

      override def decode(
        v: ArrayValue,
        idx: Int
      ): Option[FieldType[K, Option[H]] :: T] = {
        val head = hEncoder.value.decode(v, idx)
        val tail = tEncoder.value.decode(v, idx + 1)

        tail.map(t => field[K](head) :: t)
      }
    }
}
