package zio.tarantool.msgpack

import scodec.bits.{BitVector, ByteVector}
import scodec.Attempt
import zio.tarantool.msgpack.MessagePackException.UnexpectedMessagePackType

object Implicits {
  implicit class RichMessagePack(v: MessagePack) {
    def encode(): Attempt[BitVector] = MessagePackCodec.encode(v)

    /** Used for getting message size */
    final def toNumber: Long = v match {
      case MpPositiveFixInt(value) => value
      case MpUint8(value)          => value
      case MpUint16(value)         => value
      case MpUint32(value)         => value
      case MpUint64(value)         => value
      case MpInt8(value)           => value
      case MpInt16(value)          => value
      case MpInt32(value)          => value
      case MpInt64(value)          => value
      case MpNegativeFixInt(value) => value
      case _                       => throw UnexpectedMessagePackType("Not a natural number")
    }

    final def toMap: MpMap = v match {
      case v: MpMap => v
      case _        => throw UnexpectedMessagePackType("Not a MpMap")
    }
  }

  implicit class RichByteVector(v: ByteVector) {
    def decode(): Attempt[MessagePack] = MessagePackCodec.decodeValue(v.toBitVector)
  }

  implicit class RichBitVector(v: BitVector) {
    def decode(): Attempt[MessagePack] = MessagePackCodec.decodeValue(v)
  }
}
