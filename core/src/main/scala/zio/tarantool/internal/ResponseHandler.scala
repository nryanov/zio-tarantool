package zio.tarantool.internal

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableNilValueImpl
import zio._
import zio.tarantool._
import zio.tarantool.internal.RequestHandler.RequestHandler
import zio.tarantool.internal.TarantoolConnection.TarantoolConnection
import zio.tarantool.protocol.{MessagePackPacket, ResponseCode, ResponseType}

private[tarantool] object ResponseHandler {
  type ResponseHandler = Has[Service]

  private val PingData: Value = ImmutableNilValueImpl.get()

  trait Service extends Serializable {
    def start(): ZIO[Any, TarantoolError, Unit]
  }

  def start(): ZIO[ResponseHandler, TarantoolError, Unit] =
    ZIO.accessM[ResponseHandler](_.get.start())

  val live: ZLayer[
    TarantoolConnection with RequestHandler,
    TarantoolError.IOError,
    ResponseHandler
  ] = ZLayer.fromServicesManaged[
    TarantoolConnection.Service,
    RequestHandler.Service,
    Any,
    TarantoolError.IOError,
    Service
  ]((connection, requestHandler) => make(connection, requestHandler))

  def make(
    connection: TarantoolConnection.Service,
    requestHandler: RequestHandler.Service
  ): ZManaged[Any, TarantoolError.IOError, Service] = {
    val live = new Live(
      connection,
      requestHandler
    )

    for {
      _ <- live.start().forkManaged
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
