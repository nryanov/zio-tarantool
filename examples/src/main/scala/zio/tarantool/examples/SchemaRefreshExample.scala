package zio.tarantool.examples

import _root_.zio._
import zio.tarantool._
import zio.tarantool.codec.auto._

object SchemaRefreshExample extends ZIOAppDefault {
  override def run: ZIO[Any, Any, Any] = (for {
    _ <- createSpace()
    _ <- TarantoolClient.refreshMeta()
    _ <- TarantoolClient.insert.into("newSpace").tuple((1, "value")).run
    tuple <- TarantoolClient.select
      .from("newSpace")
      .index("primary")
      .key(1)
      .limit(1)
      .run
      .flatMap(_.await.flatMap(_.head[(Int, String)]))
    _ <- Console.printLine(s"Tuple: $tuple")
    _ <- TarantoolClient.eval.expression("box.space.newSpace:truncate()").run
  } yield ()).provideLayer(tarantoolLayer()).orDie

  def createSpace(): ZIO[TarantoolClient.Service, Throwable, Unit] = for {
    _ <- TarantoolClient.eval.expression("box.schema.create_space('newSpace', {if_not_exists = true})").run
    _ <- TarantoolClient.eval
      .expression(
        "box.space.newSpace:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })"
      )
      .run
  } yield ()

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (ZLayer.succeed[Clock](Clock.ClockLive) ++ config) >>> TarantoolClient.live
  }
}
