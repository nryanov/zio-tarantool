package zio.tarantool.protocol

sealed abstract class FieldKey(val value: Long)

object FieldKey {
  case object Code extends FieldKey(0x00L)
  case object Sync extends FieldKey(0x01L)
  case object SchemaId extends FieldKey(0x05L)
  case object Space extends FieldKey(0x10L)
  case object Index extends FieldKey(0x11L)
  case object Limit extends FieldKey(0x12L)
  case object Offset extends FieldKey(0x13L)
  case object Iterator extends FieldKey(0x14L)
  case object Key extends FieldKey(0x20L)
  case object Tuple extends FieldKey(0x21L)
  case object Function extends FieldKey(0x22L)
  case object Username extends FieldKey(0x23L)
  case object Expression extends FieldKey(0x27L)
  case object UpsertOps extends FieldKey(0x28L)
  case object Data extends FieldKey(0x30L)
  case object Error extends FieldKey(0x31L)
}
