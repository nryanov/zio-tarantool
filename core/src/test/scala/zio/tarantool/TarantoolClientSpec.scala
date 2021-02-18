package zio.tarantool

import java.time.Duration

import zio.test.{DefaultRunnableSpec, ZSpec, assert, suite, testM}
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.{Has, ZIO, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.TarantoolContainer.Tarantool
import zio.tarantool.builder.{TupleBuilder, UpdateOpsBuilder}
import zio.tarantool.data.TestTuple
import zio.tarantool.data.TestTuple._
import zio.tarantool.protocol.IteratorCode
import zio.tarantool.protocol.TupleEncoder._

object TarantoolClientSpec extends DefaultRunnableSpec {
  val tarantoolLayer: ZLayer[Any, Nothing, Tarantool] =
    Blocking.live >>> TarantoolContainer.tarantool()
  val configLayer: ZLayer[Tarantool, Nothing, Has[TarantoolConfig]] =
    ZLayer.fromService(container =>
      TarantoolConfig(
        host = container.container.getHost,
        port = container.container.getMappedPort(3301)
      )
    )
  val tarantoolConnectionLayer: ZLayer[Tarantool, Throwable, TarantoolConnection] =
    (Clock.live ++ configLayer) >>> TarantoolConnection.live
  val tarantoolClientLayer: ZLayer[Any with Tarantool, Throwable, TarantoolClient] =
    tarantoolConnectionLayer >>> TarantoolClient.live
  val testEnv: ZLayer[Any, Throwable, Clock with TarantoolClient] =
    Clock.live ++ (tarantoolLayer >>> tarantoolClientLayer)

  val timeoutAspect = timeout(Duration.ofSeconds(5))
  val truncateAspect = after(truncateSpace()) >>> timeoutAspect

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    (suite("TarantoolClient spec")(
      testM("should return space id using eval") {
        for {
          response <- TarantoolClient.eval("return box.space.test.id")
          result <- response.valueUnsafe[Int]
        } yield assert(result)(not(isZero))
      },
      testM("should return and decode inserted tuple") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key1", 1, 1)
          _ <- TarantoolClient.insert(spaceId, tuple)
          key <- ZIO.effect(TupleBuilder().put("key1").build().require)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          result <- response.dataUnsafe[TestTuple]
        } yield assert(result)(equalTo(Vector(tuple)))
      },
      testM("should decode deleted tuples as empty vector") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key2", 1, 1)
          _ <- TarantoolClient.insert(spaceId, tuple)
          key <- ZIO.effect(TupleBuilder().put("key2").build().require)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          insertedValue <- response.dataUnsafe[TestTuple]
          _ <- TarantoolClient.delete(spaceId, 0, key)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          deletedValue <- response.dataUnsafe[TestTuple]
        } yield assert(insertedValue)(equalTo(Vector(tuple))) &&
          assert(deletedValue)(equalTo(Vector.empty))
      },
      testM("should correctly upsert data") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key3", 1, 1)
          updateOps <- ZIO.effect(UpdateOpsBuilder().set(1, 321).add(2, 10).build().require)
          _ <- TarantoolClient.upsert(spaceId, 0, updateOps, tuple)
          key <- ZIO.effect(TupleBuilder().put("key3").build().require)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          initialValue <- response.dataUnsafe[TestTuple]
          _ <- TarantoolClient.upsert(spaceId, 0, updateOps, tuple)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          updatedValue <- response.dataUnsafe[TestTuple]
        } yield assert(initialValue)(equalTo(Vector(tuple))) &&
          assert(updatedValue)(equalTo(Vector(tuple.copy(f2 = 321, f3 = 11))))
      },
      testM("should correctly update update data") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key4", 1, 1)
          updateOps <- ZIO.effect(UpdateOpsBuilder().set(1, 123).set(2, 321).build().require)
          _ <- TarantoolClient.insert(spaceId, tuple)
          key <- ZIO.effect(TupleBuilder().put("key4").build().require)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          initialValue <- response.dataUnsafe[TestTuple]
          _ <- TarantoolClient.update(spaceId, 0, key, updateOps)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          updatedValue <- response.dataUnsafe[TestTuple]
        } yield assert(initialValue)(equalTo(Vector(tuple))) &&
          assert(updatedValue)(equalTo(Vector(tuple.copy(f2 = 123, f3 = 321))))
      },
      testM("should replace existing tuple") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key5", 1, 1)
          _ <- TarantoolClient.insert(spaceId, tuple)
          key <- ZIO.effect(TupleBuilder().put("key5").build().require)
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          initialValue <- response.dataUnsafe[TestTuple]
          _ <- TarantoolClient.replace(spaceId, tuple.copy(f2 = 12345))
          response <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          replacedValue <- response.dataUnsafe[TestTuple]
        } yield assert(initialValue)(equalTo(Vector(tuple))) && assert(replacedValue)(
          equalTo(Vector(tuple.copy(f2 = 12345)))
        )
      }
    ) @@ sequential @@ truncateAspect @@ before(createSpace().timeout(Duration.ofSeconds(5)).orDie))
      .provideCustomLayerShared(testEnv.orDie)

  private def createSpace(): ZIO[Any with TarantoolClient, Throwable, Unit] = for {
    r1 <- TarantoolClient.eval("box.schema.create_space('test', {if_not_exists = true})")
    r2 <- TarantoolClient.eval(
      "box.space.test:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'string'} })"
    )
    _ <- r1.promise.await
    _ <- r2.promise.await
  } yield ()

  private def truncateSpace(): ZIO[Any with TarantoolClient, Throwable, Unit] =
    TarantoolClient.eval("box.space.test:truncate()").flatMap(_.promise.await.unit)

  private def getSpaceId(): ZIO[Any with Clock with TarantoolClient, Throwable, Int] =
    TarantoolClient.eval("return box.space.test.id").flatMap(_.valueUnsafe[Int])
}
