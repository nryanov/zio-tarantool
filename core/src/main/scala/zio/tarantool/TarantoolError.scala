package zio.tarantool

import java.io.IOException
import java.nio.ByteBuffer

import scala.util.control.NoStackTrace

sealed trait TarantoolError extends NoStackTrace

object TarantoolError {
  final case class ConfigurationError(message: String) extends TarantoolError

  final case class SpaceNotFound(message: String) extends TarantoolError
  final case class IndexNotFound(message: String) extends TarantoolError

  final case class NotEqualSchemaId(message: String) extends TarantoolError

  final case class ProtocolError(message: String) extends TarantoolError
  final case class CodecError(exception: Throwable) extends TarantoolError
  final case class IOError(exception: IOException) extends TarantoolError
  final case class Timeout(message: String) extends TarantoolError

  final case class DirectWriteError(message: String) extends TarantoolError
  final case class MessagePackPacketReadError(message: String) extends TarantoolError

  final case class OperationException(reason: String) extends TarantoolError
  final case class NotFoundOperation(reason: String) extends TarantoolError

  private[tarantool] val toIOError: PartialFunction[Throwable, TarantoolError.IOError] = {
    case e: IOException => TarantoolError.IOError(e)
  }
}
