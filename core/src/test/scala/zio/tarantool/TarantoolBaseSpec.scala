package zio.tarantool

import zio.ZIO
import zio.clock.Clock
import zio.duration._
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolOperation
import zio.test.DefaultRunnableSpec

trait TarantoolBaseSpec extends DefaultRunnableSpec with BaseLayers {
  def awaitResponse(operation: TarantoolOperation) =
    operation.response.await
      .timeout(5.seconds)
      .flatMap(opt => ZIO.fromOption(opt))
      .orElseFail(new RuntimeException("Operation timed out"))
      .orDie

  def awaitResponseHeadOption[A: TupleEncoder](operation: TarantoolOperation) =
    awaitResponse(operation).flatMap(_.headOption[A])

  def awaitResponseData[A: TupleEncoder](operation: TarantoolOperation) =
    awaitResponse(operation).flatMap(_.resultSet[A])

  def createSpace(): ZIO[TarantoolClient, Throwable, Unit] = for {
    r1 <- TarantoolClient.eval("box.schema.create_space('test', {if_not_exists = true})")
    r2 <- TarantoolClient.eval(
      "box.space.test:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'string'} })"
    )
    _ <- r1.response.await
    _ <- r2.response.await
  } yield ()

  def createFunction() = for {
    r1 <- TarantoolClient.eval(
      "box.schema.func.create('sum', {body = [[function(a, b) return a + b end]]})"
    )
    _ <- r1.response.await
  } yield ()

  def getSpaceId(): ZIO[Clock with TarantoolClient, Throwable, Int] =
    TarantoolClient.eval("return box.space.test.id").flatMap(_.response.await.flatMap(_.head[Int]))

  def truncateSpace(): ZIO[TarantoolClient, Throwable, Unit] =
    TarantoolClient.eval("box.space.test:truncate()").flatMap(_.response.await.unit)
}
