package zio.tarantool.examples

import zio._
import zio.clock.Clock
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool._
import zio.tarantool.codec.auto._

object SchemaRefreshExample extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
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
    _ <- zio.console.putStrLn(s"Tuple: $tuple")
    _ <- TarantoolClient.eval.expression("box.space.newSpace:truncate()").run
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def createSpace(): ZIO[TarantoolClient, Throwable, Unit] = for {
    _ <- TarantoolClient.eval.expression("box.schema.create_space('newSpace', {if_not_exists = true})").run
    _ <- TarantoolClient.eval
      .expression(
        "box.space.newSpace:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })"
      )
      .run
  } yield ()

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
  }
}
