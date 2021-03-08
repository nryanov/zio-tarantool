package zio.tarantool

import java.io.IOException
import java.nio.ByteBuffer

import scala.util.control.NoStackTrace

sealed abstract class TarantoolError(msg: String, cause: Throwable)
    extends RuntimeException(msg, cause)
    with NoStackTrace

object TarantoolError {
  final case class InternalError(cause: Throwable)
      extends TarantoolError(cause.getLocalizedMessage, cause)

  final case class ConfigurationError(message: String) extends TarantoolError(message, null)

  final case class SpaceNotFound(message: String) extends TarantoolError(message, null)
  final case class IndexNotFound(message: String) extends TarantoolError(message, null)

  final case class NotEqualSchemaId(message: String) extends TarantoolError(message, null)

  final case class ProtocolError(message: String) extends TarantoolError(message, null)
  final case class CodecError(exception: Throwable)
      extends TarantoolError(exception.getLocalizedMessage, exception)
  final case class IOError(exception: IOException)
      extends TarantoolError(exception.getLocalizedMessage, exception)
  final case class Timeout(message: String) extends TarantoolError(message, null)

  final case class DirectWriteError(message: String) extends TarantoolError(message, null)
  final case class MessagePackPacketReadError(message: String) extends TarantoolError(message, null)

  final case class OperationException(reason: String) extends TarantoolError(reason, null)
  final case class NotFoundOperation(reason: String) extends TarantoolError(reason, null)

  private[tarantool] val toIOError: PartialFunction[Throwable, TarantoolError.IOError] = {
    case e: IOException => TarantoolError.IOError(e)
  }
}
