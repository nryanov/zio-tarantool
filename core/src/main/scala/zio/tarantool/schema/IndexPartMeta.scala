package zio.tarantool.schema

sealed trait IndexPartMeta

object IndexPartMeta {
  final case class SimpleIndexPartMeta(fieldNumber: Int, fieldType: String) extends IndexPartMeta

  final case class FullIndexPartMeta(
    fieldType: String,
    fieldNumber: Int,
    isNullable: Boolean,
    nullableAction: String,
    sortOrder: String
  ) extends IndexPartMeta
}
