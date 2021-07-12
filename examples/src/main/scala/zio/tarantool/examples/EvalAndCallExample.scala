package zio.tarantool.examples

import zio._
import zio.clock.Clock
import zio.tarantool._
import zio.tarantool.codec.auto._

object EvalAndCallExample extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
    eval <- TarantoolClient.eval("return 123").flatMap(_.await.flatMap(_.head[Int]))
    _ <- zio.console.putStrLn(s"Eval result: $eval")
    // create function
    _ <- TarantoolClient.eval(
      "box.schema.func.create('sum', {body = [[function(a, b) return a + b end]]})"
    )
    sum <- TarantoolClient.call("sum", (1, 2)).flatMap(_.await.flatMap(_.head[Int]))
    _ <- zio.console.putStrLn(s"Sum result: $sum")
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
  }
}
