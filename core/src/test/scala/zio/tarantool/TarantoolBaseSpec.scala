package zio.tarantool

import _root_.zio.{Promise, ZIO}
import _root_.zio.Clock
import _root_.zio.durationInt
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolResponse
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
    TarantoolClient.eval
      .expression("""
        box.schema.create_space('test', {if_not_exists = true})
        box.space.test:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'string'}})
      """)
      .run
      .flatMap(_.await.unit)

  def createFunction() = for {
    r1 <- TarantoolClient.eval
      .expression("box.schema.func.create('sum', {body = [[function(a, b) return a + b end]]})")
      .run
    _ <- r1.await
  } yield ()

  def getSpaceId(): ZIO[Clock with TarantoolClient.Service, Throwable, Int] =
    TarantoolClient.eval.expression("return box.space.test.id").run.flatMap(_.await.flatMap(_.head[Int]))

  def truncateSpace(): ZIO[TarantoolClient.Service, Throwable, Unit] =
    TarantoolClient.eval.expression("if box.space.test then box.space.test:truncate() end").run.flatMap(_.await.unit)
}
