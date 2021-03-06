package zio.tarantool.protocol

sealed trait ResponseType

object ResponseType {
  case object DataResponse extends ResponseType

  case object SqlResponse extends ResponseType

  case object ErrorResponse extends ResponseType
}
