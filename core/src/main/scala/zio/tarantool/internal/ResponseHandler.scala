package zio.tarantool.internal

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableNilValueImpl
import _root_.zio._
import zio.tarantool._
import zio.tarantool.protocol.{MessagePackPacket, ResponseCode, ResponseType}

private[tarantool] object ResponseHandler {
  private val PingData: Value = ImmutableNilValueImpl.get()

  def completePacket(
    requestHandler: RequestHandler.Service,
    packet: MessagePackPacket
  ): IO[TarantoolError, Unit] =
    for {
      syncId <- MessagePackPacket.extractSyncId(packet)
      code <- MessagePackPacket.extractCode(packet)
      _ <- completeByCode(requestHandler, code, syncId, packet).catchSome { case TarantoolError.NotFoundOperation(_) =>
        ZIO.unit
      }.tapError(err =>
        requestHandler.fail(syncId, err.getLocalizedMessage, 0).catchSome { case TarantoolError.NotFoundOperation(_) =>
          ZIO.unit
        }
      )
    } yield ()

  private def completeByCode(
    requestHandler: RequestHandler.Service,
    code: ResponseCode,
    syncId: Long,
    packet: MessagePackPacket
  ): IO[TarantoolError, Unit] = code match {
    case ResponseCode.Success     => completeSucceeded(requestHandler, syncId, packet)
    case ResponseCode.Error(code) => completeFailed(requestHandler, syncId, packet, code)
  }

  private def completeSucceeded(
    requestHandler: RequestHandler.Service,
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
        completeFailed(requestHandler, syncId, packet, 0)
    }
  } yield ()

  private def completeFailed(
    requestHandler: RequestHandler.Service,
    syncId: Long,
    packet: MessagePackPacket,
    errorCode: Int
  ): IO[TarantoolError, Unit] =
    MessagePackPacket.extractError(packet).flatMap(error => requestHandler.fail(syncId, error, errorCode))
}
