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

    def typeName(): String = v match {
      case _: MpPositiveFixInt => "MpPositiveFixInt"
      case _: MpFixString      => "MpFixString"
      case MpNil               => "MpNil"
      case _: MpBinary8        => "MpBinary8"
      case _: MpBinary16       => "MpBinary16"
      case _: MpBinary32       => "MpBinary32"
      case _: MpExtension8     => "MpExtension8"
      case _: MpExtension16    => "MpExtension16"
      case _: MpExtension32    => "MpExtension32"
      case _: MpFloat32        => "MpFloat32"
      case _: MpFloat64        => "MpFloat64"
      case _: MpUint8          => "MpUint8"
      case _: MpUint16         => "MpUint16"
      case _: MpUint32         => "MpUint32"
      case _: MpUint64         => "MpUint64"
      case _: MpInt8           => "MpInt8"
      case _: MpInt16          => "MpInt16"
      case _: MpInt32          => "MpInt32"
      case _: MpInt64          => "MpInt64"
      case _: MpFixExtension1  => "MpFixExtension1"
      case _: MpFixExtension2  => "MpFixExtension2"
      case _: MpFixExtension4  => "MpFixExtension4"
      case _: MpFixExtension8  => "MpFixExtension8"
      case _: MpFixExtension16 => "MpFixExtension16"
      case _: MpString8        => "MpString8"
      case _: MpString16       => "MpString16"
      case _: MpString32       => "MpString32"
      case _: MpNegativeFixInt => "MpNegativeFixInt"
    }
  }

  implicit class RichByteVector(v: ByteVector) {
    def decode(): Attempt[MessagePack] = MessagePackCodec.decodeValue(v.toBitVector)
  }

  implicit class RichBitVector(v: BitVector) {
    def decode(): Attempt[MessagePack] = MessagePackCodec.decodeValue(v)
  }
}
