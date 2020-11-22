package zio.tarantool

import java.io.IOException

import zio.tarantool.protocol.{AuthInfo, MessagePackPacket, OperationCode}
import zio.tarantool.msgpack._
import zio._
import zio.macros.accessible

@accessible
object TarantoolConnection {

  type TarantoolConnectionService = Has[Service]

  trait Service {
    def connect(): ZIO[Any, IOException, Unit]

    def connect(authInfo: AuthInfo): ZIO[Any, IOException, Unit]

    def createPacket(
      op: OperationCode,
      schemaId: Option[Long],
      body: MpMap
    ): MessagePackPacket

    def readPacket(): ZIO[Any, Throwable, MessagePackPacket]

    def writePacket(packet: MessagePackPacket): ZIO[Any, Throwable, Int]
  }
}
