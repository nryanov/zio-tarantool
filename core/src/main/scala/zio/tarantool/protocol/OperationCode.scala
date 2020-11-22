package zio.tarantool.protocol

import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

sealed abstract class OperationCode(val value: Int) extends IntEnumEntry

object OperationCode extends IntEnum[OperationCode] {
  override def values: immutable.IndexedSeq[OperationCode] = findValues

  case object Select extends OperationCode(0x01)
  case object Insert extends OperationCode(0x02)
  case object Replace extends OperationCode(0x03)
  case object Update extends OperationCode(0x04)
  case object Delete extends OperationCode(0x05)
  @Deprecated(since = "tarantool 1.6") case object OldCall extends OperationCode(0x06)
  case object Auth extends OperationCode(0x07)
  case object Eval extends OperationCode(0x08)
  case object Upsert extends OperationCode(0x09)
  case object Call extends OperationCode(0x0A)
  case object Execute extends OperationCode(0x0B)
  case object NOP extends OperationCode(0x0C)
  case object Prepare extends OperationCode(0x0D)
  case object Ping extends OperationCode(0x40)
  case object Confirm extends OperationCode(0x28)
  case object Rollback extends OperationCode(0x29)
}
