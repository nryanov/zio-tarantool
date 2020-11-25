package zio.tarantool.protocol

import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

// https://github.com/tarantool/tarantool/blob/444213178c6260d6adfd640f7e4a0c5e6f8f2458/src/box/errcode.h#L54
sealed abstract class Code(val value: Int) extends IntEnumEntry

object Code extends IntEnum[Code] {
  override def values: immutable.IndexedSeq[Code] = findValues

  case object Success extends Code(0x0)

  case object ErrorTypeMarker extends Code(0x8000)

  case object ReadOnly extends Code(7)

  case object Timeout extends Code(78)

  case object WrongSchemaVersion extends Code(109)

  case object Loading extends Code(116)

  case object LocalInstanceIdIsReadOnly extends Code(128)

}
