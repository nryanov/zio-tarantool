package zio.tarantool.protocol

sealed abstract class RequestCode(val value: Int)

object RequestCode {
  case object Select extends RequestCode(0x01)
  case object Insert extends RequestCode(0x02)
  case object Replace extends RequestCode(0x03)
  case object Update extends RequestCode(0x04)
  case object Delete extends RequestCode(0x05)
  @Deprecated // Tarantool prior to 1.6
  case object OldCall extends RequestCode(0x06)
  case object Auth extends RequestCode(0x07)
  case object Eval extends RequestCode(0x08)
  case object Upsert extends RequestCode(0x09)
  case object Call extends RequestCode(0x0a)
  case object NOP extends RequestCode(0x0c)
  case object Ping extends RequestCode(0x40)
  case object Confirm extends RequestCode(0x28)
  case object Rollback extends RequestCode(0x29)

  /* sql codes */
  case object Execute extends RequestCode(0x0b)
  case object Prepare extends RequestCode(0x0d)
}
