package zio.tarantool.core

import zio._
import zio.logging._
import zio.tarantool.protocol._
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.TarantoolError

object TarantoolCommunicationInterceptor {
  type TarantoolCommunicationInterceptor = Has[Service]

  trait Service {
    def submitRequest(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation]
  }

  private[this] final class Live(
    logger: Logger[String],
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: TarantoolRequestHandler.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: Ref[Long]
  ) extends Service {
    override def submitRequest(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation] = for {
      schemaId <- schemaMetaManager.schemaVersion
      syncId <- syncIdProvider.updateAndGet(_ + 1)
      request = TarantoolRequest(op, syncId, schemaId, body)
      operation <- requestHandler.submitRequest(request)
      packet <- TarantoolRequest.createPacket(request)
      _ <- connection.send(packet)
    } yield operation
  }
}
