package zio.tarantool.codec

import org.msgpack.core.MessagePack
import org.msgpack.value.Value
import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.MessagePackPacket

object MessagePackPacketSerDe {

  def serialize(packet: MessagePackPacket): IO[TarantoolError.CodecError, Array[Byte]] =
    ZIO.effect {
      val packer = MessagePack.newDefaultBufferPacker()
      val header = Encoder[Map[Long, Value]].encode(packet.header)
      val body = Encoder[Map[Long, Value]].encode(packet.body)
      packer.packValue(header)
      packer.packValue(body)
      packer.close()
      packer.toByteArray
    }.mapError(TarantoolError.CodecError)

  def deserialize(data: Array[Byte]): MessagePackPacket = {
    val unpacker = MessagePack.newDefaultUnpacker(data)
    val headerMp = unpacker.unpackValue()
    val bodyMp = unpacker.unpackValue()
    unpacker.close()

    val header = Encoder[Map[Long, Value]].decode(headerMp)
    val body = Encoder[Map[Long, Value]].decode(bodyMp)

    MessagePackPacket(header, body)
  }

}
