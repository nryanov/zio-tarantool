package zio.tarantool.core

import zio._
import zio.logging._
import zio.macros.accessible
import zio.tarantool.protocol._
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.{TarantoolError, protocol}
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.ResponseHandler.ResponseHandler
import zio.tarantool.core.SchemaMetaManager.SchemaMetaManager
import zio.tarantool.core.SyncIdProvider.SyncIdProvider
import zio.tarantool.core.TarantoolConnection.TarantoolConnection
import zio.tarantool.core.schema.{IndexMeta, SpaceMeta}

@accessible[CommunicationFacade.Service]
object CommunicationFacade {
  type CommunicationFacade = Has[Service]

  trait Service {
    def submitRequest(
      op: RequestCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation]

    def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta]

    def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta]
  }

  val live: ZLayer[
    Logging with SchemaMetaManager with RequestHandler with ResponseHandler with TarantoolConnection with SyncIdProvider,
    TarantoolError,
    CommunicationFacade
  ] = ZLayer.fromServicesManaged[
    SchemaMetaManager.Service,
    RequestHandler.Service,
    ResponseHandler.Service,
    TarantoolConnection.Service,
    SyncIdProvider.Service,
    Logging,
    TarantoolError,
    Service
  ] {
    (
      schemaMetaManager,
      requestHandler,
      responseHandler,
      connection,
      syncIdProvider
    ) =>
      make(
        schemaMetaManager,
        requestHandler,
        responseHandler,
        connection,
        syncIdProvider
      )
  }

  def make(
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: RequestHandler.Service,
    responseHandler: ResponseHandler.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ): ZManaged[Logging, TarantoolError, Service] = ZManaged.fromEffect {
    for {
      logger <- ZIO.service[Logger[String]]
    } yield new Live(
      logger,
      schemaMetaManager,
      requestHandler,
      connection,
      syncIdProvider
    )
  }

  private[tarantool] class Live(
    logger: Logger[String],
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: RequestHandler.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ) extends Service {
    override def submitRequest(
      op: RequestCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation] = for {
      schemaId <- schemaMetaManager.schemaId
      syncId <- syncIdProvider.syncId()
      request = protocol.TarantoolRequest(op, syncId, Some(schemaId), body)
      operation <- requestHandler.submitRequest(request)
      packet <- TarantoolRequest.createPacket(request)
      _ <- connection.sendRequest(packet)
    } yield operation

    override def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta] =
      schemaMetaManager.getSpaceMeta(spaceName)

    override def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta] =
      schemaMetaManager.getIndexMeta(spaceName, indexName)
  }
}
