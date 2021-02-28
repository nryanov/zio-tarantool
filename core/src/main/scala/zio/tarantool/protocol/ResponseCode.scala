package zio.tarantool.protocol

// https://github.com/tarantool/tarantool/blob/444213178c6260d6adfd640f7e4a0c5e6f8f2458/src/box/errcode.h#L54
sealed abstract class ResponseCode(val value: Int)

object ResponseCode {
  case object Success extends ResponseCode(0x0)
  case object ErrorTypeMarker extends ResponseCode(0x8000)
  case object ReadOnly extends ResponseCode(7)
  case object Timeout extends ResponseCode(78)
  case object WrongSchemaVersion extends ResponseCode(109)
  case object Loading extends ResponseCode(116)
  case object LocalInstanceIdIsReadOnly extends ResponseCode(128)

}
