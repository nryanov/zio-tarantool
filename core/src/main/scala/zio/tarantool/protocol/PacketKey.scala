package zio.tarantool.protocol

sealed abstract class Header(val value: Long)

object Header {
  case object Code extends Header(0x00L)
  case object Sync extends Header(0x01L)
  case object SchemaId extends Header(0x05L)
}

sealed abstract class ResponseCode(val value: Int)

object ResponseCode {
  case object Success extends ResponseCode(0x0)
  // https://github.com/tarantool/tarantool/blob/444213178c6260d6adfd640f7e4a0c5e6f8f2458/src/box/errcode.h#L54
  case object ErrorTypeMarker extends ResponseCode(0x8000)
  case object ReadOnly extends ResponseCode(7)
  case object Timeout extends ResponseCode(78)
  case object WrongSchemaVersion extends ResponseCode(109)
  case object Loading extends ResponseCode(116)
  case object LocalInstanceIdIsReadOnly extends ResponseCode(128)

}

sealed abstract class RequestBodyKey(val value: Long)

object RequestBodyKey {
  case object Space extends RequestBodyKey(0x10L)
  case object Index extends RequestBodyKey(0x11L)
  case object Limit extends RequestBodyKey(0x12L)
  case object Offset extends RequestBodyKey(0x13L)
  case object Iterator extends RequestBodyKey(0x14L)
  case object Key extends RequestBodyKey(0x20L)
  case object Tuple extends RequestBodyKey(0x21L)
  case object Function extends RequestBodyKey(0x22L)
  case object Username extends RequestBodyKey(0x23L)
  case object Expression extends RequestBodyKey(0x27L)
  case object UpsertOps extends RequestBodyKey(0x28L)
}

sealed abstract class RequestSqlBodyKey(val value: Long)

object RequestSqlBodyKey {
  case object BindMetadata extends RequestSqlBodyKey(0x33L)
  case object BindCount extends RequestSqlBodyKey(0x34L)

  case object SqlText extends RequestSqlBodyKey(0x40L)
  case object SqlBind extends RequestSqlBodyKey(0x41L)
  case object StatementId extends RequestSqlBodyKey(0x43L)

  case object Options extends RequestSqlBodyKey(0x2bL)
}

sealed abstract class ResponseBodyKey(val value: Long)

object ResponseBodyKey {
  case object Data extends ResponseBodyKey(0x30L)
  case object Error24 extends ResponseBodyKey(0x31L)
  // https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-error-type
  case object Error extends ResponseBodyKey(0x52L)

  case object SqlInfo extends ResponseBodyKey(0x42L)
  case object Metadata extends ResponseBodyKey(0x32L)
}

sealed abstract class SqlMetadata(val value: Long)

object SqlMetadata {
  case object FieldName extends SqlMetadata(0x00L)
  case object FieldType extends SqlMetadata(0x01L)
  case object FieldColl extends SqlMetadata(0x02L)
  case object FieldIsNullable extends SqlMetadata(0x03L)
  case object FieldIsAutoIncrement extends SqlMetadata(0x04L)
  case object FieldSpan extends SqlMetadata(0x05L)
}
