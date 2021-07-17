package zio.tarantool

import java.io.IOException

import zio.tarantool.protocol.{MessagePackPacket, RequestCode, ResponseCode}

sealed abstract class TarantoolError(message: String, cause: Throwable)
    extends Exception(message, cause)

object TarantoolError {
  final case class IOError(exception: IOException)
      extends TarantoolError(exception.getLocalizedMessage, exception)
  final case class InternalError(cause: Throwable)
      extends TarantoolError(cause.getLocalizedMessage, cause)

  final case class NotSupportedUpdateOperation(msg: String) extends TarantoolError(msg, null)

  final case class AuthError(message: String, code: ResponseCode)
      extends TarantoolError(s"$message. Code: $code", null)

  final case class SpaceNotFound(space: String) extends TarantoolError(space, null)
  final case class IndexNotFound(space: String, index: String)
      extends TarantoolError(s"$space:$index", null)

  final case class ProtocolError(message: String) extends TarantoolError(message, null)
  final case class CodecError(exception: Throwable)
      extends TarantoolError(exception.getLocalizedMessage, exception)
  case object EmptyResultSet extends TarantoolError("Empty result set", null)
  final case class Timeout(message: String) extends TarantoolError(message, null)
  final case class UnknownResponseCode(mp: MessagePackPacket)
      extends TarantoolError("Unknown response code", null)

  final case class OperationException(reason: String, errorCode: Int)
      extends TarantoolError(s"[$errorCode] $reason", null)
  final case class NotFoundOperation(syncId: Long) extends TarantoolError(syncId.toString, null)
  final case class DuplicateOperation(syncId: Long) extends TarantoolError(syncId.toString, null)
  final case class DeclinedOperation(syncId: Long, code: RequestCode)
      extends TarantoolError(s"$code -- $syncId", null)

  private[tarantool] val toIOError: PartialFunction[Throwable, TarantoolError.IOError] = {
    case e: IOException => TarantoolError.IOError(e)
  }
}
