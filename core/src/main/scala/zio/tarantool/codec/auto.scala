package zio.tarantool.codec

import scodec.{Attempt, Err}
import shapeless._
import shapeless.labelled.{FieldType, field}
import zio.tarantool.msgpack._

object auto extends LowPriorityInstances

private[tarantool] trait LowPriorityInstances extends LowestPriorityInstances {
  final implicit def genericFamilyEncoder[A, H <: Coproduct](implicit
    gen: LabelledGeneric.Aux[A, H],
    hEncoder: Lazy[TupleEncoder[H]],
    notOption: A <:!< Option[Z] forSome { type Z }
  ): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(v: A): Attempt[MpArray] = hEncoder.value.encode(gen.to(v))

    override def decode(v: MpArray, idx: Int): Attempt[A] =
      hEncoder.value.decode(v, idx).map(h => gen.from(h))
  }

  final implicit val cnilEncoder: TupleEncoder[CNil] = new TupleEncoder[CNil] {
    override def encode(v: CNil): Attempt[MpArray] = Attempt.successful(MpFixArray(Vector.empty))

    override def decode(v: MpArray, idx: Int): Attempt[CNil] = Attempt.failure(Err("Unexpected"))
  }

  final implicit def coproductEncoder[K <: Symbol, H, T <: Coproduct](implicit
    hEncoder: Lazy[TupleEncoder[H]],
    tEncoder: Lazy[TupleEncoder[T]]
  ): TupleEncoder[FieldType[K, H] :+: T] = new TupleEncoder[FieldType[K, H] :+: T] {
    override def encode(v: FieldType[K, H] :+: T): Attempt[MpArray] =
      v match {
        case Inl(head) => hEncoder.value.encode(head)
        case Inr(tail) => tEncoder.value.encode(tail)
      }

    override def decode(v: MpArray, idx: Int): Attempt[FieldType[K, H] :+: T] =
      hEncoder.value
        .decode(v, idx)
        .map(r => Inl(field[K](r)))
        .orElse(tEncoder.value.decode(v, idx).map(r => Inr(r)))
  }

  implicit def genericEncoder[A, H <: HList](implicit
    gen: LabelledGeneric.Aux[A, H],
    hEncoder: Lazy[TupleEncoder[H]]
  ): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(v: A): Attempt[MpArray] = hEncoder.value.encode(gen.to(v))

    override def decode(v: MpArray, idx: Int): Attempt[A] =
      hEncoder.value.decode(v, idx).map(gen.from)
  }

  implicit val hnilEncoder: TupleEncoder[HNil] = new TupleEncoder[HNil] {
    override def encode(v: HNil): Attempt[MpArray] = Attempt.successful(MpFixArray(Vector.empty))

    override def decode(v: MpArray, idx: Int): Attempt[HNil] = Attempt.successful(HNil)
  }

  implicit def hlistEncoder[K <: Symbol, H, T <: HList](implicit
    hEncoder: Lazy[TupleEncoder[H]],
    tEncoder: Lazy[TupleEncoder[T]]
  ): TupleEncoder[FieldType[K, H] :: T] = new TupleEncoder[FieldType[K, H] :: T] {
    override def encode(v: FieldType[K, H] :: T): Attempt[MpArray] = v match {
      case h :: t =>
        val head: Attempt[MpArray] = hEncoder.value.encode(h)
        val tail: Attempt[MpArray] = tEncoder.value.encode(t)

        for {
          h <- head
          t <- tail
          vector = h.value.++(t.value)
        } yield
          if (vector.length <= 15) MpFixArray(vector)
          else if (vector.length <= 65535) MpArray16(vector)
          else MpArray32(vector)
    }

    override def decode(v: MpArray, idx: Int): Attempt[FieldType[K, H] :: T] = for {
      head <- hEncoder.value.decode(v, idx)
      tail <- tEncoder.value.decode(v, idx + 1)
    } yield field[K](head) :: tail
  }
}

private[tarantool] trait LowestPriorityInstances {
  implicit def genericOptionEncoder[A, H <: HList](implicit
    gen: LabelledGeneric.Aux[A, H],
    hEncoder: Lazy[TupleEncoder[Option[H]]]
  ): TupleEncoder[Option[A]] = new TupleEncoder[Option[A]] {
    override def encode(v: Option[A]): Attempt[MpArray] = v match {
      case value @ Some(_) => hEncoder.value.encode(value.map(gen.to))
      case None            => Attempt.successful(MpFixArray(Vector.empty))
    }

    override def decode(v: MpArray, idx: Int): Attempt[Option[A]] =
      if (v.value.nonEmpty) {
        hEncoder.value.decode(v, idx).map(_.map(gen.from))
      } else {
        Attempt.successful(None)
      }
  }

  implicit val hnilOptionEncoder: TupleEncoder[Option[HNil]] = new TupleEncoder[Option[HNil]] {
    override def encode(v: Option[HNil]): Attempt[MpArray] =
      Attempt.successful(MpFixArray(Vector.empty))

    override def decode(v: MpArray, idx: Int): Attempt[Option[HNil]] =
      Attempt.successful(Some(HNil))
  }

  implicit def hlistOptionEncoder1[K <: Symbol, H, T <: HList](implicit
    hEncoder: Lazy[TupleEncoder[Option[H]]],
    tEncoder: Lazy[TupleEncoder[Option[T]]],
    notOption: H <:!< Option[Z] forSome { type Z }
  ): TupleEncoder[Option[FieldType[K, H] :: T]] = new TupleEncoder[Option[FieldType[K, H] :: T]] {
    override def encode(v: Option[FieldType[K, H] :: T]): Attempt[MpArray] = {
      def split[A](v: Option[H :: T])(f: (Option[H], Option[T]) => A): A = v.fold(f(None, None))({
        case h :: t => f(Some(h), Some(t))
      })

      split(v) { case (head, tail) =>
        val encodedHead: Attempt[MpArray] = hEncoder.value.encode(head)
        val encodedTail: Attempt[MpArray] = tEncoder.value.encode(tail)

        for {
          h <- encodedHead
          t <- encodedTail
          vector = h.value.++(t.value)
        } yield
          if (vector.length <= 15) MpFixArray(vector)
          else if (vector.length <= 65535) MpArray16(vector)
          else MpArray32(vector)
      }
    }

    override def decode(v: MpArray, idx: Int): Attempt[Option[FieldType[K, H] :: T]] = for {
      head <- hEncoder.value.decode(v, idx)
      tail <- tEncoder.value.decode(v, idx + 1)
    } yield head.flatMap { h =>
      tail.map(t => field[K](h) :: t)
    }
  }

  implicit def hlistOptionEncoder2[K <: Symbol, H, T <: HList](implicit
    hEncoder: Lazy[TupleEncoder[Option[H]]],
    tEncoder: Lazy[TupleEncoder[Option[T]]]
  ): TupleEncoder[Option[FieldType[K, Option[H]] :: T]] =
    new TupleEncoder[Option[FieldType[K, Option[H]] :: T]] {
      override def encode(v: Option[FieldType[K, Option[H]] :: T]): Attempt[MpArray] = {
        def split[A](v: Option[Option[H] :: T])(f: (Option[H], Option[T]) => A): A =
          v.fold(f(None, None))({ case h :: t => f(h, Some(t)) })

        split(v) { case (head, tail) =>
          val encodedHead: Attempt[MpArray] = hEncoder.value.encode(head)
          val encodedTail: Attempt[MpArray] = tEncoder.value.encode(tail)

          for {
            h <- encodedHead
            t <- encodedTail
            vector = h.value.++(t.value)
          } yield
            if (vector.length <= 15) MpFixArray(vector)
            else if (vector.length <= 65535) MpArray16(vector)
            else MpArray32(vector)
        }
      }

      override def decode(v: MpArray, idx: Int): Attempt[Option[FieldType[K, Option[H]] :: T]] =
        for {
          head <- hEncoder.value.decode(v, idx)
          tail <- tEncoder.value.decode(v, idx + 1)
        } yield tail.map(t => field[K](head) :: t)
    }
}
