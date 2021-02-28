package zio.tarantool.core

import zio._
import zio.macros.accessible

@accessible
object SchemaIdProvider {
  type SchemaIdProvider = Has[Service]

  trait Service {
    def schemaId: UIO[Long]

    def updateSchemaId(schemaId: Long): UIO[Unit]
  }

  val live: ZLayer[Any, Nothing, SchemaIdProvider] = ZLayer.fromManaged(make())

  def make(): ZManaged[Any, Nothing, Service] = Ref.make(0L).map(new Live(_)).toManaged_

  private[this] final class Live(ref: Ref[Long]) extends Service {
    override def schemaId: UIO[Long] = ref.get

    override def updateSchemaId(schemaId: Long): UIO[Unit] = ref.set(schemaId)
  }
}
