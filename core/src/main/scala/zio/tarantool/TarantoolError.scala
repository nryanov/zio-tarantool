package zio.tarantool

import java.io.IOException

import zio.tarantool.protocol.{MessagePackPacket, RequestCode, ResponseCode}

sealed abstract class TarantoolError(message: String, cause: Option[Throwable]) extends Exception(message, cause.orNull)

object TarantoolError {
  final case class IOError(exception: IOException)
      extends TarantoolError(exception.getLocalizedMessage, Some(exception))
  final case class InternalError(cause: Throwable) extends TarantoolError(cause.getLocalizedMessage, Some(cause))

  final case class NotSupportedUpdateOperation(msg: String) extends TarantoolError(msg, None)

  final case class AuthError(message: String, code: ResponseCode) extends TarantoolError(s"$message. Code: $code", None)

  final case class SpaceNotFound(space: String) extends TarantoolError(space, None)
  final case class IndexNotFound(space: String, index: String) extends TarantoolError(s"$space:$index", None)

  final case class ProtocolError(message: String) extends TarantoolError(message, None)
  final case class CodecError(exception: Throwable)
      extends TarantoolError(exception.getLocalizedMessage, Some(exception))
  case object EmptyResultSet extends TarantoolError("Empty result set", None)
  final case class Timeout(message: String) extends TarantoolError(message, None)
  final case class UnknownResponseCode(mp: MessagePackPacket) extends TarantoolError("Unknown response code", None)

  final case class OperationException(reason: String, errorCode: Int)
      extends TarantoolError(s"[$errorCode] $reason", None)
  final case class NotFoundOperation(syncId: Long) extends TarantoolError(syncId.toString, None)
  final case class DuplicateOperation(syncId: Long) extends TarantoolError(syncId.toString, None)
  final case class DeclinedOperation(syncId: Long, code: RequestCode) extends TarantoolError(s"$code -- $syncId", None)

  private[tarantool] val toIOError: PartialFunction[Throwable, TarantoolError.IOError] = { case e: IOException =>
    TarantoolError.IOError(e)
  }
}
