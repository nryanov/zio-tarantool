package zio.tarantool.core

import scodec.bits.ByteVector
import zio.macros.accessible
import zio.{Has, IO, ULayer, ZManaged}
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.MessagePackPacket
import zio.tarantool.codec.MessagePackPacketCodec
import zio.tarantool.msgpack.{MessagePack, MessagePackCodec}

@accessible
private[tarantool] object PacketManager {
  type PacketManager = Has[Service]

  trait Service extends Serializable {
    def decodeToMessagePack(vector: ByteVector): IO[TarantoolError.CodecError, MessagePack]

    def decodeToMessagePackPacket(
      vector: ByteVector
    ): IO[TarantoolError.CodecError, MessagePackPacket]
  }

  val live: ULayer[PacketManager] = make().toLayer

  def make(): ZManaged[Any, Nothing, Service] = ZManaged.succeed(new Live)

  private[this] final class Live extends Service {
    override def decodeToMessagePack(v: ByteVector): IO[TarantoolError.CodecError, MessagePack] =
      IO.effect(MessagePackCodec.decodeValue(v.toBitVector).require)
        .mapError(TarantoolError.CodecError)

    override def decodeToMessagePackPacket(
      vector: ByteVector
    ): IO[TarantoolError.CodecError, MessagePackPacket] = IO
      .effect(
        MessagePackPacketCodec.decodeValue(vector.toBitVector).require
      )
      .mapError(TarantoolError.CodecError)
  }

}
