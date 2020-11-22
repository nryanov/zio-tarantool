package zio.tarantool.protocol

import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable

sealed abstract class Key(val value: Int) extends IntEnumEntry

object Key extends IntEnum[Key] {
  override def values: immutable.IndexedSeq[Key] = findValues

  case object Code extends Key(0x00)
  case object Sync extends Key(0x01)
  case object SchemaId extends Key(0x05)
  case object Space extends Key(0x10)
  case object Index extends Key(0x11)
  case object Limit extends Key(0x12)
  case object Offset extends Key(0x13)
  case object Iterator extends Key(0x14)
  case object Key extends Key(0x20)
  case object Tuple extends Key(0x21)
  case object Function extends Key(0x22)
  case object Username extends Key(0x23)
  case object Expression extends Key(0x27)
  case object UpsertOps extends Key(0x28)
  case object Data extends Key(0x30)
  case object Error extends Key(0x31)
}
