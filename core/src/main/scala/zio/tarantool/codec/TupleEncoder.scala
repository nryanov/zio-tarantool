package zio.tarantool.codec

import org.msgpack.value.{ArrayValue, Value}
import org.msgpack.value.impl.ImmutableNilValueImpl
import zio.tarantool.TarantoolError.CodecError

trait TupleEncoder[A] extends Serializable {
  def encode(v: A): Vector[Value]

  def decode(v: ArrayValue, idx: Int): A
}

object TupleEncoder {
  def apply[A](implicit instance: TupleEncoder[A]): instance.type = instance

  implicit val unitEncoder: TupleEncoder[Unit] = new TupleEncoder[Unit] {
    override def encode(v: Unit): Vector[Value] = Vector(ImmutableNilValueImpl.get())

    override def decode(v: ArrayValue, idx: Int): Unit = {
      val value = v.get(idx)
      if (!value.isNilValue) {
        throw CodecError(new IllegalArgumentException(s"Expect MPNil, got: $value"))
      }
    }
  }

  implicit val valueEncoder: TupleEncoder[Value] = new TupleEncoder[Value] {
    override def encode(v: Value): Vector[Value] = Vector(v)

    override def decode(v: ArrayValue, idx: Int): Value = v.get(idx)
  }

  implicit def fromEncoder[A](implicit encoder: Encoder[A]): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(v: A): Vector[Value] = Vector(encoder.encode(v))

    override def decode(v: ArrayValue, idx: Int): A = encoder.decode(v.get(idx))
  }

  implicit def fromEncoderOption[A](implicit encoder: Encoder[A]): TupleEncoder[Option[A]] =
    new TupleEncoder[Option[A]] {
      override def encode(v: Option[A]): Vector[Value] =
        v match {
          case Some(value) => Vector(encoder.encode(value))
          case None        => Vector(ImmutableNilValueImpl.get())
        }

      override def decode(v: ArrayValue, idx: Int): Option[A] = {
        val value = v.get(idx)
        if (value.isNilValue) {
          None
        } else {
          Some(encoder.decode(value))
        }
      }
    }
}
