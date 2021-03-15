package zio.tarantool.core

import zio._
import zio.logging._
import zio.macros.accessible
import zio.tarantool._
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.SchemaMetaManager.SchemaMetaManager
import zio.tarantool.core.TarantoolConnection.TarantoolConnection
import zio.tarantool.protocol.{
  MessagePackPacket,
  ResponseCode,
  ResponseType,
  TarantoolRequest,
  TarantoolResponse
}

@accessible[ResponseHandler.Service]
private[tarantool] object ResponseHandler {
  type ResponseHandler = Has[Service]

  trait Service extends Serializable {
    def start(): ZIO[Any, TarantoolError, Unit]

    def complete(packet: MessagePackPacket): IO[TarantoolError, Unit]
  }

  val live: ZLayer[
    TarantoolConnection with SchemaMetaManager with RequestHandler with Logging,
    TarantoolError.IOError,
    ResponseHandler
  ] =
    ZLayer.fromServicesManaged[
      TarantoolConnection.Service,
      SchemaMetaManager.Service,
      RequestHandler.Service,
      Logging,
      TarantoolError.IOError,
      Service
    ]((connection, schemaMetaManager, requestHandler) =>
      make(connection, schemaMetaManager, requestHandler)
    )

  def make(
    connection: TarantoolConnection.Service,
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: RequestHandler.Service
  ): ZManaged[Logging, TarantoolError.IOError, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      live = new Live(
        logger,
        connection,
        schemaMetaManager,
        requestHandler
      )
      _ <- live.start().forkManaged
    } yield live

  private[tarantool] class Live(
    logger: Logger[String],
    connection: TarantoolConnection.Service,
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: RequestHandler.Service
  ) extends Service {

    override def start(): ZIO[Any, TarantoolError, Unit] =
      connection
        .receive()
        .foreach(mp =>
          //todo: remove (or debug?)
          logger.info(s"Read packet: $mp") *>
            complete(mp).onError(err =>
              logger.error("Error happened while trying to complete operation", err)
            )
        )
        .retryWhile(_ => true)

    override def complete(packet: MessagePackPacket): IO[TarantoolError, Unit] =
      for {
        syncId <- MessagePackPacket.extractSyncId(packet)
        _ <- logger.debug(s"Complete operation with id: $syncId")
        schemaId <- MessagePackPacket.extractSchemaId(packet)
        code <- MessagePackPacket.extractCode(packet)
        _ <- completeByCode(code, syncId, schemaId, packet).tapError(err =>
          requestHandler.fail(syncId, err.getLocalizedMessage)
        )
      } yield ()

    private def completeByCode(
      code: Long,
      syncId: Long,
      schemaId: Long,
      packet: MessagePackPacket
    ) = code match {
      case ResponseCode.Success.value            => completeSucceeded(schemaId, syncId, packet)
      case ResponseCode.WrongSchemaVersion.value => reschedule(syncId, schemaId)
      case _                                     => completeFailed(syncId, packet)
    }

    private def completeSucceeded(schemaId: Long, syncId: Long, packet: MessagePackPacket) = for {
      responseType <- MessagePackPacket.responseType(packet)
      _ <- responseType match {
        case ResponseType.DataResponse =>
          MessagePackPacket
            .extractData(packet)
            .flatMap(data => requestHandler.complete(syncId, TarantoolResponse(schemaId, data)))
        case ResponseType.SqlResponse =>
          MessagePackPacket
            .extractSql(packet)
            .flatMap(data => requestHandler.complete(syncId, TarantoolResponse(schemaId, data)))
        case ResponseType.ErrorResponse =>
          IO.fail(TarantoolError.OperationException("Unexpected error in packet with SUCCEED_CODE"))
            .zipRight(completeFailed(syncId, packet))
      }
    } yield ()

    private def completeFailed(syncId: Long, packet: MessagePackPacket) =
      MessagePackPacket.extractError(packet).flatMap(error => requestHandler.fail(syncId, error))

    private def reschedule(syncId: Long, newSchemaId: Long): ZIO[Any, TarantoolError, Unit] = for {
      cachedSchemaId <- schemaMetaManager.schemaId
      _ <- ZIO.when(newSchemaId > cachedSchemaId)(schemaMetaManager.refresh)
      op <- requestHandler.rescheduleRequest(syncId, newSchemaId)
      packet <- TarantoolRequest.createPacket(op.request)
      _ <- connection.sendRequest(packet)
    } yield ()
  }

}
