package zio.tarantool

import _root_.zio.{Promise, ZIO}
import _root_.zio.Clock
import _root_.zio.durationInt
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.schema.IndexPart.ByPosition
import _root_.zio.test.ZIOSpecDefault

trait TarantoolBaseSpec extends ZIOSpecDefault with BaseLayers {
  def awaitResponse(operation: Promise[TarantoolError, TarantoolResponse]) =
    operation.await
      .timeout(5.seconds)
      .flatMap(opt => ZIO.fromOption(opt))
      .orElseFail(new RuntimeException("Operation timed out"))
      .orDie

  def awaitResponseHeadOption[A: TupleEncoder](
    operation: Promise[TarantoolError, TarantoolResponse]
  ) =
    awaitResponse(operation).flatMap(_.headOption[A])

  def awaitResponseData[A: TupleEncoder](operation: Promise[TarantoolError, TarantoolResponse]) =
    awaitResponse(operation).flatMap(_.resultSet[A])

  def createSpace(): ZIO[TarantoolClient.Service, Throwable, Unit] =
    for {
      _ <- TarantoolClient.schema.createSpace("test").ifNotExists(true).run
      _ <- TarantoolClient.schema
        .createIndex("test", "primary")
        .unique(true)
        .ifNotExists(true)
        .parts(ByPosition(1, "string"))
        .run
    } yield ()

  def createFunction() = for {
    r1 <- TarantoolClient.eval
      .expression("box.schema.func.create('sum', {body = [[function(a, b) return a + b end]]})")
      .run
    _ <- r1.await
  } yield ()

  def getSpaceId(): ZIO[Clock with TarantoolClient.Service, Throwable, Int] =
    TarantoolClient.eval.expression("return box.space.test.id").run.flatMap(_.await.flatMap(_.head[Int]))

  def truncateSpace(): ZIO[TarantoolClient.Service, Throwable, Unit] =
    TarantoolClient.schema.truncate("test").run
}
