package zio.tarantool.protocol

import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

// https://github.com/tarantool/tarantool/blob/444213178c6260d6adfd640f7e4a0c5e6f8f2458/src/box/iterator_type.h
sealed abstract class IteratorCode(val value: Int) extends IntEnumEntry

object IteratorCode extends IntEnum[IteratorCode] {
  override def values: immutable.IndexedSeq[IteratorCode] = findValues

  case object Eq extends IteratorCode(0x00)
  case object Req extends IteratorCode(0x01)
  case object All extends IteratorCode(0x02)
  case object Lt extends IteratorCode(0x03)
  case object Le extends IteratorCode(0x04)
  case object Ge extends IteratorCode(0x05)
  case object Gt extends IteratorCode(0x06)
  case object BitsAllSet extends IteratorCode(0x07)
  case object BitsAnySet extends IteratorCode(0x08)
  case object BitsAllNotSet extends IteratorCode(0x09)
  case object Overlaps extends IteratorCode(0x0a)
  case object Neighbor extends IteratorCode(0x0b)
}
