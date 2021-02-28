package zio.tarantool

import java.time.Duration

import zio._
import zio.clock.Clock
import zio.test.{DefaultRunnableSpec, ZSpec, assert, suite, testM}
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.builder.{TupleBuilder, UpdateOpsBuilder}
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.data.TestTuple
import zio.tarantool.data.TestTuple._
import zio.tarantool.protocol.{IteratorCode, TarantoolOperation}
import zio.tarantool.codec.TupleEncoder._

object TarantoolClientSpec extends DefaultRunnableSpec with BaseLayers {
  val testEnv: ZLayer[Any, Throwable, Clock with TarantoolClient] =
    Clock.live ++ tarantoolClientLayer

  val timeoutAspect = timeout(Duration.ofSeconds(5))
  val truncateAspect = after(truncateSpace()) >>> timeoutAspect

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    (suite("TarantoolClient spec")(
      testM("should return space id using eval") {
        for {
          operation <- TarantoolClient.eval("return box.space.test.id")
          result <- awaitResponseValue[Int](operation)
        } yield assert(result)(not(isZero))
      },
      testM("should return and decode inserted tuple") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key1", 1, 1)
          _ <- TarantoolClient.insert(spaceId, tuple)
          key <- ZIO.effect(TupleBuilder().put("key1").build().require)
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          result <- awaitResponseData[TestTuple](operation)
        } yield assert(result)(equalTo(Vector(tuple)))
      },
      testM("should decode deleted tuples as empty vector") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key2", 1, 1)
          _ <- TarantoolClient.insert(spaceId, tuple)
          key <- ZIO.effect(TupleBuilder().put("key2").build().require)
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          insertedValue <- awaitResponseData[TestTuple](operation)
          _ <- TarantoolClient.delete(spaceId, 0, key)
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          deletedValue <- awaitResponseData[TestTuple](operation)
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
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          initialValue <- awaitResponseData[TestTuple](operation)
          _ <- TarantoolClient.upsert(spaceId, 0, updateOps, tuple)
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          updatedValue <- awaitResponseData[TestTuple](operation)
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
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          initialValue <- awaitResponseData[TestTuple](operation)
          _ <- TarantoolClient.update(spaceId, 0, key, updateOps)
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          updatedValue <- awaitResponseData[TestTuple](operation)
        } yield assert(initialValue)(equalTo(Vector(tuple))) &&
          assert(updatedValue)(equalTo(Vector(tuple.copy(f2 = 123, f3 = 321))))
      },
      testM("should replace existing tuple") {
        for {
          spaceId <- getSpaceId()
          tuple = TestTuple("key5", 1, 1)
          _ <- TarantoolClient.insert(spaceId, tuple)
          key <- ZIO.effect(TupleBuilder().put("key5").build().require)
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          initialValue <- awaitResponseData[TestTuple](operation)
          _ <- TarantoolClient.replace(spaceId, tuple.copy(f2 = 12345))
          operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
          replacedValue <- awaitResponseData[TestTuple](operation)
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
    _ <- r1.response.await
    _ <- r2.response.await
  } yield ()

  private def awaitResponseValue[A: TupleEncoder](operation: TarantoolOperation) =
    operation.response.await.flatMap(_.valueUnsafe[A])

  private def awaitResponseData[A: TupleEncoder](operation: TarantoolOperation) =
    operation.response.await.flatMap(_.dataUnsafe[A])

  private def truncateSpace(): ZIO[Any with TarantoolClient, Throwable, Unit] =
    TarantoolClient.eval("box.space.test:truncate()").flatMap(_.response.await.unit)

  private def getSpaceId(): ZIO[Any with Clock with TarantoolClient, Throwable, Int] =
    TarantoolClient
      .eval("return box.space.test.id")
      .flatMap(_.response.await.flatMap(_.valueUnsafe[Int]))
}
