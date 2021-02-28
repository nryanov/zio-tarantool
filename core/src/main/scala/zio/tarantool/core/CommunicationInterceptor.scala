package zio.tarantool.core

import zio._
import zio.logging._
import zio.tarantool.protocol._
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.TarantoolError
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.ResponseHandler.ResponseHandler
import zio.tarantool.core.SchemaIdProvider.SchemaIdProvider
import zio.tarantool.core.SocketChannelQueuedWriter.SocketChannelQueuedWriter
import zio.tarantool.core.SyncIdProvider.SyncIdProvider

object CommunicationInterceptor {
  type CommunicationInterceptor = Has[Service]

  trait Service {
    def submitRequest(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation]
  }

  val live: ZLayer[
    Logging with SchemaIdProvider with RequestHandler with ResponseHandler with SocketChannelQueuedWriter with SyncIdProvider,
    TarantoolError,
    CommunicationInterceptor
  ] = ZLayer.fromServicesManaged[
    SchemaIdProvider.Service,
    RequestHandler.Service,
    ResponseHandler.Service,
    SocketChannelQueuedWriter.Service,
    SyncIdProvider.Service,
    Logging,
    TarantoolError,
    Service
  ] { (schemaIdProviderLayer, requestHandler, responseHandler, queuedWriter, syncIdProvider) =>
    make(schemaIdProviderLayer, requestHandler, responseHandler, queuedWriter, syncIdProvider)
  }

  def make(
    schemaIdProviderLayer: SchemaIdProvider.Service,
    requestHandler: RequestHandler.Service,
    responseHandler: ResponseHandler.Service,
    queuedWriter: SocketChannelQueuedWriter.Service,
    syncIdProvider: SyncIdProvider.Service
  ): ZManaged[Logging, TarantoolError, Service] = ZManaged.fromEffect {
    for {
      logger <- ZIO.service[Logger[String]]
      _ <- responseHandler.start()
    } yield new Live(logger, schemaIdProviderLayer, requestHandler, queuedWriter, syncIdProvider)
  }

  private[this] final class Live(
    logger: Logger[String],
    schemaIdProviderLayer: SchemaIdProvider.Service,
    requestHandler: RequestHandler.Service,
    queuedWriter: SocketChannelQueuedWriter.Service,
    syncIdProvider: SyncIdProvider.Service
  ) extends Service {
    override def submitRequest(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation] = for {
      schemaId <- schemaIdProviderLayer.schemaId
      syncId <- syncIdProvider.syncId()
      request = TarantoolRequest(op, syncId, Some(schemaId), body)
      operation <- requestHandler.submitRequest(request)
      packet <- TarantoolRequest.createPacket(request)
      _ <- queuedWriter.send(packet)
    } yield operation
  }
}
