package zio.tarantool

import java.io.IOException

import zio.tarantool.protocol.{MessagePackPacket, RequestCode}

import scala.util.control.NoStackTrace

sealed trait TarantoolError extends NoStackTrace

object TarantoolError {
  final case class IOError(exception: IOException) extends TarantoolError
  final case class InternalError(cause: Throwable) extends TarantoolError

  final case class AuthError(message: String) extends TarantoolError

  final case class SpaceNotFound(space: String) extends TarantoolError
  final case class IndexNotFound(space: String, index: String) extends TarantoolError

  final case class ProtocolError(message: String) extends TarantoolError
  final case class CodecError(exception: Throwable) extends TarantoolError
  case object EmptyResultSet extends TarantoolError
  final case class Timeout(message: String) extends TarantoolError
  final case class UnknownResponseCode(mp: MessagePackPacket) extends TarantoolError

  final case class OperationException(reason: String, errorCode: Int) extends TarantoolError
  final case class NotFoundOperation(syncId: Long) extends TarantoolError
  final case class DuplicateOperation(syncId: Long) extends TarantoolError
  final case class DeclinedOperation(syncId: Long, code: RequestCode) extends TarantoolError

  private[tarantool] val toIOError: PartialFunction[Throwable, TarantoolError.IOError] = {
    case e: IOException => TarantoolError.IOError(e)
  }
}
