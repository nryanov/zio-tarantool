package zio.tarantool

import java.nio.ByteBuffer

import zio.tarantool.impl.PacketManagerLive
import zio.{Has, RIO, Task, ULayer, ZIO, ZLayer}
import zio.tarantool.msgpack.{Encoder, MessagePack, MpMap}
import zio.tarantool.protocol.{MessagePackPacket, OperationCode}

object PacketManager {
  type PacketManager = Has[Service]

  trait Service {
    def createPacket(
      op: OperationCode,
      syncId: Long,
      schemaId: Option[Long],
      body: MpMap
    ): ZIO[Any, Throwable, MessagePackPacket]

    def toBuffer(packet: MessagePackPacket): ZIO[Any, Throwable, ByteBuffer]

    def extractCode(packet: MessagePackPacket): ZIO[Any, Throwable, Long]

    def extractError(packet: MessagePackPacket): ZIO[Any, Throwable, String]

    def extractData(packet: MessagePackPacket): ZIO[Any, Throwable, MessagePack]

  }

  val live: ULayer[PacketManager] = ZLayer.succeed(new PacketManagerLive)

  def createPacket(op: OperationCode, syncId: Long, schemaId: Option[Long], body: MpMap): RIO[PacketManager, MessagePackPacket] =
    ZIO.accessM(_.get.createPacket(op, syncId, schemaId, body))

  def toBuffer(packet: MessagePackPacket): RIO[PacketManager, ByteBuffer] =
    ZIO.accessM(_.get.toBuffer(packet))

  def extractCode(packet: MessagePackPacket): RIO[PacketManager, Long] =
    ZIO.accessM(_.get.extractCode(packet))

  def extractError(packet: MessagePackPacket): RIO[PacketManager, String] =
    ZIO.accessM(_.get.extractError(packet))

  def extractData(packet: MessagePackPacket): RIO[PacketManager, MessagePack] =
    ZIO.accessM(_.get.extractData(packet))

}
