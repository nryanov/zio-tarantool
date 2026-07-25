package zio.tarantool.examples

import _root_.zio._
import zio.tarantool._
import zio.tarantool.codec.auto._
import zio.tarantool.schema.IndexPart.ByPosition

object SchemaRefreshExample extends ZIOAppDefault {
  override def run: ZIO[Any, Any, Any] = (for {
    _ <- TarantoolClient.schema.createSpace("newSpace").ifNotExists(true).run
    _ <- TarantoolClient.schema
      .createIndex("newSpace", "primary")
      .unique(true)
      .ifNotExists(true)
      .parts(ByPosition(1, "number"))
      .run
    _ <- TarantoolClient.insert.into("newSpace").tuple((1, "value")).run
    tuple <- TarantoolClient.select
      .from("newSpace")
      .index("primary")
      .key(1)
      .limit(1)
      .run
      .flatMap(_.await.flatMap(_.head[(Int, String)]))
    _ <- Console.printLine(s"Tuple: $tuple")
    _ <- TarantoolClient.schema.truncate("newSpace").run
  } yield ()).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (ZLayer.succeed[Clock](Clock.ClockLive) ++ config) >>> TarantoolClient.live
  }
}
