package zio.tarantool.msgpack

import scala.util.control.NoStackTrace

object MessagePackException {
  final case class UnexpectedMessagePackType(reason: String) extends RuntimeException(reason) with NoStackTrace

  final case class MessagePackEncodingException(cause: Throwable) extends RuntimeException(cause) with NoStackTrace
}
