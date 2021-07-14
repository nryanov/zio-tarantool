package zio.tarantool.internal.schema

private[tarantool] sealed trait IndexPartMeta

private[tarantool] object IndexPartMeta {
  final case class SimpleIndexPartMeta(fieldNumber: Int, fieldType: String) extends IndexPartMeta

  final case class FullIndexPartMeta(
    fieldType: String,
    fieldNumber: Int,
    isNullable: Boolean,
    nullableAction: String,
    sortOrder: String
  ) extends IndexPartMeta
}
