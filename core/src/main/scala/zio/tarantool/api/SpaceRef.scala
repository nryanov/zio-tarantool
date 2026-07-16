package zio.tarantool.api

sealed trait SpaceRef

object SpaceRef {
  final case class Id(id: Int) extends SpaceRef
  final case class Name(name: String) extends SpaceRef
}
