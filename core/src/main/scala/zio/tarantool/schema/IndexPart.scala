package zio.tarantool.schema

/** Index part definition used when creating an index. */
sealed trait IndexPart

object IndexPart {
  final case class ByPosition(field: Int, fieldType: String) extends IndexPart

  final case class ByName(field: String, fieldType: String) extends IndexPart
}
