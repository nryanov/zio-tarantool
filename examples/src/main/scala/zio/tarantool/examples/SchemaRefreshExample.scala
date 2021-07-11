package zio.tarantool.examples

import zio._
import zio.clock.Clock
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool._
import zio.tarantool.codec.auto._
import zio.tarantool.protocol.IteratorCode

object SchemaRefreshExample extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
    _ <- createSpace()
    _ <- TarantoolClient.refreshMeta()
    _ <- TarantoolClient.insert("newSpace", (1, "value"))
    tuple <- TarantoolClient
      .select("newSpace", "primary", 1, 0, IteratorCode.Eq, 1)
      .flatMap(_.await.flatMap(_.head[(Int, String)]))
    _ <- zio.console.putStrLn(s"Tuple: $tuple")
    _ <- TarantoolClient.eval("box.space.newSpace:truncate()")
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def createSpace(): ZIO[TarantoolClient, Throwable, Unit] = for {
    _ <- TarantoolClient.eval("box.schema.create_space('newSpace', {if_not_exists = true})")
    _ <- TarantoolClient.eval(
      "box.space.newSpace:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })"
    )
  } yield ()

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
  }
}
