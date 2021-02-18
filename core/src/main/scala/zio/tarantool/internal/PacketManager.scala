package zio.tarantool.internal

import java.nio.ByteBuffer

import scodec.bits.ByteVector
import zio.macros.accessible
import zio.tarantool.TarantoolError
import zio.tarantool.internal.impl.PacketManagerLive
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.protocol.{MessagePackPacket, OperationCode}
import zio.{Has, IO, RIO, ULayer, ZIO, ZManaged}

@accessible
private[tarantool] object PacketManager {
  type PacketManager = Has[Service]

  trait Service extends Serializable {
    def decodeToMessagePack(vector: ByteVector): IO[TarantoolError.CodecError, MessagePack]

    def decodeToMessagePackPacket(
      vector: ByteVector
    ): IO[TarantoolError.CodecError, MessagePackPacket]

    def createPacket(
      op: OperationCode,
      syncId: Long,
      schemaId: Option[Long],
      body: Map[Long, MessagePack]
    ): IO[TarantoolError.CodecError, MessagePackPacket]

    def toBuffer(packet: MessagePackPacket): IO[TarantoolError, ByteBuffer]

    def extractCode(packet: MessagePackPacket): IO[TarantoolError, Long]

    def extractError(packet: MessagePackPacket): IO[TarantoolError, String]

    def extractData(packet: MessagePackPacket): IO[TarantoolError.ProtocolError, MessagePack]

    def extractSyncId(packet: MessagePackPacket): IO[Throwable, Long]

  }

  val live: ULayer[PacketManager] = make().toLayer

  def make(): ZManaged[Any, Nothing, Service] = ZManaged.succeed(new PacketManagerLive)

}
