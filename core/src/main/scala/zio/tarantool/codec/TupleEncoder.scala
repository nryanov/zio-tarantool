package zio.tarantool.codec

import scodec.{Attempt, Err}
import zio.tarantool.msgpack.{Encoder, MpArray, MpFixArray}

trait TupleEncoder[A] extends Serializable {
  def encode(v: A): Attempt[MpArray]

  def decode(v: MpArray, idx: Int): Attempt[A]

  final def encodeUnsafe(v: A): MpArray = encode(v).require

  final def decodeUnsafe(v: MpArray, idx: Int): A = decode(v, idx).require
}

object TupleEncoder {
  def apply[A](implicit instance: TupleEncoder[A]): instance.type = instance

  implicit val unitEncoder: TupleEncoder[Unit] = new TupleEncoder[Unit] {
    override def encode(v: Unit): Attempt[MpArray] = Attempt.successful(MpFixArray(Vector.empty))

    override def decode(v: MpArray, idx: Int): Attempt[Unit] =
      if (v.value.isEmpty) Attempt.successful(())
      else Attempt.failure(Err("Non empty vector for unit value"))
  }

  implicit val mpArrayEncoder: TupleEncoder[MpArray] = new TupleEncoder[MpArray] {
    override def encode(v: MpArray): Attempt[MpArray] = Attempt.successful(v)

    override def decode(v: MpArray, idx: Int): Attempt[MpArray] = Attempt.successful(v)
  }

  implicit def fromEncoder[A](implicit encoder: Encoder[A]): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(v: A): Attempt[MpArray] =
      encoder.encode(v).map(res => MpFixArray(Vector(res)))

    override def decode(v: MpArray, idx: Int): Attempt[A] = encoder.decode(v.value(idx))
  }

  implicit def fromEncoderOption[A](implicit encoder: Encoder[A]): TupleEncoder[Option[A]] =
    new TupleEncoder[Option[A]] {
      override def encode(v: Option[A]): Attempt[MpArray] = v match {
        case Some(value) => encoder.encode(value).map(value => MpFixArray(Vector(value)))
        case None        => Attempt.successful(MpFixArray(Vector.empty))
      }

      override def decode(v: MpArray, idx: Int): Attempt[Option[A]] = v match {
        case msg: MpArray if msg.value.nonEmpty => encoder.decode(msg.value(idx)).map(Some(_))
        case msg: MpArray if msg.value.isEmpty  => Attempt.successful(None)
      }
    }
}
