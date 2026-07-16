package zio.tarantool.api

sealed trait IndexRef

object IndexRef {
  final case class Id(id: Int) extends IndexRef
  final case class Name(name: String) extends IndexRef
}
