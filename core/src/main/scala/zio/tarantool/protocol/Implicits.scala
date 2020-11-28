package zio.tarantool.protocol

import scodec.Attempt
import scodec.bits.BitVector

object Implicits {
  implicit class RichMessagePackPacket(v: MessagePackPacket) {
    def encode(): Attempt[BitVector] = MessagePackPacketCodec.encode(v)
  }
}
