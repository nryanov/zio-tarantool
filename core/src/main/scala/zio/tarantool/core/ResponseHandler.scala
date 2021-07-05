package zio.tarantool.core

import zio._
import zio.logging._
import zio.tarantool._
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.TarantoolConnection.TarantoolConnection
import zio.tarantool.msgpack.MpFixArray
import zio.tarantool.protocol.{MessagePackPacket, ResponseCode, ResponseType}

private[tarantool] object ResponseHandler {
  type ResponseHandler = Has[Service]

  private val PingData = MpFixArray(Vector.empty)

  trait Service extends Serializable {
    def start(): ZIO[Any, TarantoolError, Unit]
  }

  def start(): ZIO[ResponseHandler, TarantoolError, Unit] =
    ZIO.accessM[ResponseHandler](_.get.start())

  val live: ZLayer[
    TarantoolConnection with RequestHandler with Logging,
    TarantoolError.IOError,
    ResponseHandler
  ] =
    ZLayer.fromServicesManaged[
      TarantoolConnection.Service,
      RequestHandler.Service,
      Logging,
      TarantoolError.IOError,
      Service
    ]((connection, requestHandler) => make(connection, requestHandler))

  def make(
    connection: TarantoolConnection.Service,
    requestHandler: RequestHandler.Service
  ): ZManaged[Logging, TarantoolError.IOError, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      live = new Live(
        logger,
        connection,
        requestHandler
      )
      _ <- live.start().forkManaged
    } yield live

  private[tarantool] class Live(
    logger: Logger[String],
    connection: TarantoolConnection.Service,
    requestHandler: RequestHandler.Service
  ) extends Service {

    override def start(): ZIO[Any, TarantoolError, Unit] =
      connection
        .receive()
        .foreach(mp =>
          complete(mp).onError(err =>
            logger.error(s"Error happened while trying to complete operation. Packet: $mp", err)
          )
        )
        .forever

    private def complete(packet: MessagePackPacket): IO[TarantoolError, Unit] =
      for {
        syncId <- MessagePackPacket.extractSyncId(packet)
        _ <- logger.debug(s"Complete operation with id: $syncId")
        code <- MessagePackPacket.extractCode(packet)
        _ <- completeByCode(code, syncId, packet).tapError(err =>
          requestHandler.fail(syncId, err.getLocalizedMessage, 0)
        )
      } yield ()

    private def completeByCode(
      code: ResponseCode,
      syncId: Long,
      packet: MessagePackPacket
    ): IO[TarantoolError, Unit] = code match {
      case ResponseCode.Success     => completeSucceeded(syncId, packet)
      case ResponseCode.Error(code) => completeFailed(syncId, packet, code)
    }

    private def completeSucceeded(
      syncId: Long,
      packet: MessagePackPacket
    ): IO[TarantoolError, Unit] = for {
      responseType <- MessagePackPacket.responseType(packet)
      _ <- responseType match {
        case ResponseType.DataResponse =>
          MessagePackPacket
            .extractData(packet)
            .flatMap(data => requestHandler.complete(syncId, data))
        case ResponseType.SqlResponse =>
          MessagePackPacket
            .extractSql(packet)
            .flatMap(data => requestHandler.complete(syncId, data))
        case ResponseType.PingResponse =>
          requestHandler.complete(syncId, PingData)
        case ResponseType.ErrorResponse =>
          // Unexpected error in packet with SUCCEED_CODE
          completeFailed(syncId, packet, 0)
      }
    } yield ()

    private def completeFailed(
      syncId: Long,
      packet: MessagePackPacket,
      errorCode: Int
    ): IO[TarantoolError, Unit] =
      MessagePackPacket
        .extractError(packet)
        .flatMap(error => requestHandler.fail(syncId, error, errorCode))
  }

}
