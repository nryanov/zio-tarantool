package zio.tarantool.internal.schema

private[tarantool] final case class FieldMeta(
  fieldName: String,
  fieldType: String,
  isNullable: Boolean
)

private[tarantool] object FieldMeta {
  def apply(fieldName: String, fieldType: String, isNullable: Boolean): FieldMeta =
    new FieldMeta(fieldName, fieldType, isNullable)

  def apply(fieldName: String, fieldType: String): FieldMeta =
    new FieldMeta(fieldName, fieldType, false)
}
