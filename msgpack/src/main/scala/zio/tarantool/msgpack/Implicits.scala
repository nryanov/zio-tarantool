package zio.tarantool.msgpack

import scodec.bits.{BitVector, ByteVector}
import scodec.{Attempt, Codec}

object Implicits {
  implicit class RichMessagePack(v: MessagePack) {
    def encode(): Attempt[BitVector] = MessagePackCodec.encode(v)
  }

  implicit class RichByteVector(v: ByteVector) {
    def decode(): Attempt[MessagePack] = MessagePackCodec.decodeValue(v.toBitVector)
  }

  implicit class RichBitVector(v: BitVector) {
    def decode(): Attempt[MessagePack] = MessagePackCodec.decodeValue(v)
  }
}
