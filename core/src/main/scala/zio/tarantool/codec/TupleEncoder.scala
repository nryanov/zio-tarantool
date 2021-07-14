package zio.tarantool.codec

import org.msgpack.core.MessageUnpacker
import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableNilValueImpl

trait TupleEncoder[A] extends Serializable {
  def encode(v: A): Vector[Value]

  def decode(unpacker: MessageUnpacker): A
}

object TupleEncoder {
  def apply[A](implicit instance: TupleEncoder[A]): instance.type = instance

  implicit val unitEncoder: TupleEncoder[Unit] = new TupleEncoder[Unit] {
    override def encode(v: Unit): Vector[Value] = Vector(ImmutableNilValueImpl.get())

    override def decode(unpacker: MessageUnpacker): Unit = unpacker.unpackNil()
  }

  implicit val valueEncoder: TupleEncoder[Value] = new TupleEncoder[Value] {
    override def encode(v: Value): Vector[Value] = Vector(v)

    override def decode(unpacker: MessageUnpacker): Value = unpacker.unpackValue()
  }

  implicit def fromEncoder[A](implicit encoder: Encoder[A]): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(v: A): Vector[Value] = Vector(encoder.encode(v))

    override def decode(unpacker: MessageUnpacker): A = encoder.decode(unpacker.unpackValue())
  }

  implicit def fromEncoderOption[A](implicit encoder: Encoder[A]): TupleEncoder[Option[A]] =
    new TupleEncoder[Option[A]] {
      override def encode(v: Option[A]): Vector[Value] =
        v match {
          case Some(value) => Vector(encoder.encode(value))
          case None        => Vector(ImmutableNilValueImpl.get())
        }

      override def decode(unpacker: MessageUnpacker): Option[A] = {
        val value = unpacker.unpackValue()
        if (value.isNilValue) {
          None
        } else {
          Some(encoder.decode(value))
        }
      }
    }
}
