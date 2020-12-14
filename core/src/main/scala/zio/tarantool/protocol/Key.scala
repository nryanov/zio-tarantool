package zio.tarantool.protocol

sealed abstract class Key(val value: Long)

object Key {
  case object Code extends Key(0x00L)
  case object Sync extends Key(0x01L)
  case object SchemaId extends Key(0x05L)
  case object Space extends Key(0x10L)
  case object Index extends Key(0x11L)
  case object Limit extends Key(0x12L)
  case object Offset extends Key(0x13L)
  case object Iterator extends Key(0x14L)
  case object Key extends Key(0x20L)
  case object Tuple extends Key(0x21L)
  case object Function extends Key(0x22L)
  case object Username extends Key(0x23L)
  case object Expression extends Key(0x27L)
  case object UpsertOps extends Key(0x28L)
  case object Data extends Key(0x30L)
  case object Error extends Key(0x31L)
}
