package zio.tarantool.schema

final case class FieldMeta(
  fieldName: String,
  fieldType: String,
  isNullable: Boolean
)

object FieldMeta {
  def apply(fieldName: String, fieldType: String): FieldMeta =
    FieldMeta(fieldName, fieldType, isNullable = false)
}
