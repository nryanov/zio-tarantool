package zio.tarantool.internal

import java.nio.ByteBuffer

import zio.tarantool.internal.impl.PacketManagerLive
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.protocol.{MessagePackPacket, OperationCode}
import zio.{Has, RIO, ULayer, ZIO, ZManaged}

private[tarantool] object PacketManager {
  type PacketManager = Has[Service]

  trait Service extends Serializable {
    def createPacket(
      op: OperationCode,
      syncId: Long,
      schemaId: Option[Long],
      body: Map[Long, MessagePack]
    ): ZIO[Any, Throwable, MessagePackPacket]

    def toBuffer(packet: MessagePackPacket): ZIO[Any, Throwable, ByteBuffer]

    def extractCode(packet: MessagePackPacket): ZIO[Any, Throwable, Long]

    def extractError(packet: MessagePackPacket): ZIO[Any, Throwable, String]

    def extractData(packet: MessagePackPacket): ZIO[Any, Throwable, MessagePack]

    def extractSyncId(packet: MessagePackPacket): ZIO[Any, Throwable, Long]

  }

  def live(): ULayer[PacketManager] = make().toLayer

  def make(): ZManaged[Any, Nothing, Service] = ZManaged.succeed(new PacketManagerLive)

  def createPacket(
    op: OperationCode,
    syncId: Long,
    schemaId: Option[Long],
    body: Map[Long, MessagePack]
  ): RIO[PacketManager, MessagePackPacket] =
    ZIO.accessM(_.get.createPacket(op, syncId, schemaId, body))

  def toBuffer(packet: MessagePackPacket): RIO[PacketManager, ByteBuffer] =
    ZIO.accessM(_.get.toBuffer(packet))

  def extractCode(packet: MessagePackPacket): RIO[PacketManager, Long] =
    ZIO.accessM(_.get.extractCode(packet))

  def extractError(packet: MessagePackPacket): RIO[PacketManager, String] =
    ZIO.accessM(_.get.extractError(packet))

  def extractData(packet: MessagePackPacket): RIO[PacketManager, MessagePack] =
    ZIO.accessM(_.get.extractData(packet))

  def extractSyncId(packet: MessagePackPacket): RIO[PacketManager, Long] =
    ZIO.accessM(_.get.extractSyncId(packet))

}
