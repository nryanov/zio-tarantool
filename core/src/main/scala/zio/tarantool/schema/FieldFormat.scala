package zio.tarantool.schema

/** Field definition used when creating a space format. */
final case class FieldFormat(
  name: String,
  fieldType: String,
  isNullable: Boolean = false
)
