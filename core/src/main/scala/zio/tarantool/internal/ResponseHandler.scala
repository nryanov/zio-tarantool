package zio.tarantool.internal

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableNilValueImpl
import _root_.zio._
import zio.tarantool._
import zio.tarantool.protocol.{MessagePackPacket, ResponseCode, ResponseType}

private[tarantool] object ResponseHandler {
  private val PingData: Value = ImmutableNilValueImpl.get()

  trait Service extends Serializable {
    def start(): ZIO[Any, TarantoolError, Unit]
  }

  def start(): ZIO[Service, TarantoolError, Unit] =
    ZIO.serviceWithZIO(_.start())

  val live: ZLayer[TarantoolConnection.Service with RequestHandler.Service, Nothing, Service] =
    ZLayer.scoped {
      for {
        connection <- ZIO.service[TarantoolConnection.Service]
        requestHandler <- ZIO.service[RequestHandler.Service]
        service <- make(connection, requestHandler)
      } yield service
    }

  def make(
    connection: TarantoolConnection.Service,
    requestHandler: RequestHandler.Service
  ): ZIO[Scope, Nothing, Service] = {
    val live = new Live(connection, requestHandler)

    for {
      _ <- live.start().forkScoped
    } yield live
  }

  private[tarantool] class Live(
    connection: TarantoolConnection.Service,
    requestHandler: RequestHandler.Service
  ) extends Service {

    override def start(): ZIO[Any, TarantoolError, Unit] =
      connection
        .receive()
        .foreach(mp =>
          complete(mp)
            .onError(err => sys.error(s"Error happened while trying to complete operation: $err \n Packet: $mp"))
        )
        .forever

    private def complete(packet: MessagePackPacket): IO[TarantoolError, Unit] =
      for {
        syncId <- MessagePackPacket.extractSyncId(packet)
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
          MessagePackPacket.extractData(packet).flatMap(data => requestHandler.complete(syncId, data))
        case ResponseType.SqlResponse =>
          MessagePackPacket.extractSql(packet).flatMap(data => requestHandler.complete(syncId, data))
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
      MessagePackPacket.extractError(packet).flatMap(error => requestHandler.fail(syncId, error, errorCode))
  }

}
