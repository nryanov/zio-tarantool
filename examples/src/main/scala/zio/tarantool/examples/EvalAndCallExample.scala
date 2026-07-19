package zio.tarantool.examples

import _root_.zio._
import zio.tarantool._
import zio.tarantool.codec.auto._

object EvalAndCallExample extends ZIOAppDefault {
  override def run: ZIO[Any, Any, Any] = (for {
    eval <- TarantoolClient.eval.expression("return 123").run.flatMap(_.await.flatMap(_.head[Int]))
    _ <- Console.printLine(s"Eval result: $eval")
    // create function
    _ <- TarantoolClient.eval
      .expression("box.schema.func.create('sum', {body = [[function(a, b) return a + b end]]})")
      .run
    sum <- TarantoolClient.call.function("sum").args((1, 2)).run.flatMap(_.await.flatMap(_.head[Int]))
    _ <- Console.printLine(s"Sum result: $sum")
  } yield ()).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (ZLayer.succeed[Clock](Clock.ClockLive) ++ config) >>> TarantoolClient.live
  }
}
