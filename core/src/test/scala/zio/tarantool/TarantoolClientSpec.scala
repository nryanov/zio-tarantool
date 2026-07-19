package zio.tarantool

import java.time.Duration

import _root_.zio._
import _root_.zio.Clock
import _root_.zio.test._
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect._
import zio.tarantool.codec.auto._
import zio.tarantool.codec.TupleOpsBuilder
import zio.tarantool.data.{TestTuple, TestTupleKey}
import zio.tarantool.data.TestTuple._

object TarantoolClientSpec extends TarantoolBaseSpec {
  private val tuple = TestTuple("key1", 1, 1)
  private val key = TestTupleKey("key1")

  final case class SumFunctionArgs(a: Int, b: Int)

  private val eval = test("eval") {
    for {
      operation <- TarantoolClient.eval.expression("return 123").run
      result <- awaitResponse(operation).flatMap(_.head[Int])
    } yield assert(result)(equalTo(123))
  }

  private val call = test("call") {
    for {
      operation <- TarantoolClient.call.function("sum").args(SumFunctionArgs(2, 5)).run
      result <- awaitResponse(operation).flatMap(_.head[Int])
    } yield assert(result)(equalTo(7))
  }

  private val insert = test("insert") {
    for {
      spaceId <- ZIO.service[Int]
      _ <- TarantoolClient.insert.into(spaceId).tuple(tuple).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      result <- awaitResponseData[TestTuple](operation)
    } yield assert(result)(equalTo(Vector(tuple)))
  }

  private val upsert = test("upsert") {
    for {
      spaceId <- ZIO.service[Int]
      ops <- TupleOpsBuilder[TestTuple].assign("f2", 321).assign("f3", 11).buildM()
      _ <- TarantoolClient.upsert.into(spaceId).index(0).ops(ops).tuple(tuple).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      initialValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.upsert.into(spaceId).index(0).ops(ops).tuple(tuple).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      updatedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(initialValue)(equalTo(Vector(tuple))) &&
      assert(updatedValue)(equalTo(Vector(tuple.copy(f2 = 321, f3 = 11))))
  }

  private val delete = test("delete") {
    for {
      spaceId <- ZIO.service[Int]
      _ <- TarantoolClient.insert.into(spaceId).tuple(tuple).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      insertedValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.delete.from(spaceId).index(0).key(key).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      deletedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(insertedValue)(equalTo(Vector(tuple))) &&
      assert(deletedValue)(equalTo(Vector.empty))
  }

  private val update = test("update") {
    for {
      spaceId <- ZIO.service[Int]
      ops <- TupleOpsBuilder[TestTuple].assign("f2", 123).assign("f3", 321).buildM()
      _ <- TarantoolClient.insert.into(spaceId).tuple(tuple).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      initialValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.update.in(spaceId).index(0).key(key).ops(ops).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      updatedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(initialValue)(equalTo(Vector(tuple))) &&
      assert(updatedValue)(equalTo(Vector(tuple.copy(f2 = 123, f3 = 321))))
  }

  private val replace = test("replace") {
    for {
      spaceId <- ZIO.service[Int]
      _ <- TarantoolClient.insert.into(spaceId).tuple(tuple).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      initialValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.replace.into(spaceId).tuple(tuple.copy(f2 = 12345)).run
      operation <- TarantoolClient.select.from(spaceId).index(0).key(key).limit(1).run
      replacedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(initialValue)(equalTo(Vector(tuple))) && assert(replacedValue)(
      equalTo(Vector(tuple.copy(f2 = 12345)))
    )
  }

  private val executeSqlStatement = test("execute sql statement") {
    for {
      query1 <- TarantoolClient.execute
        .sql("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))")
        .run
      query2 <- TarantoolClient.execute.sql("INSERT INTO table1 VALUES (1, 'A')").run
      query3 <- TarantoolClient.execute.sql("SELECT * FROM table1 WHERE column1 = 1").run
      _ <- awaitResponse(query1)
      _ <- awaitResponse(query2)
      _ <- awaitResponse(query3)
    } yield assert(true)(isTrue)
  }

  override def spec: Spec[TestEnvironment, Any] =
    (suite("TarantoolClient without schema meta cache")(
      eval,
      call,
      insert,
      delete,
      upsert,
      update,
      replace,
      executeSqlStatement
    ) @@ sequential @@ after(truncateSpace())).provideLayerShared(createSharedLayer())

  private def createSharedLayer() = {
    val client = tarantoolClientLayer
    val clock = ZLayer.succeed[Clock](Clock.ClockLive)

    val prepare: ZLayer[TarantoolClient.Service with Clock, Nothing, Int] = ZLayer.fromZIO {
      (for {
        _ <- createSpace()
        _ <- createFunction()
        spaceId <- getSpaceId()
      } yield spaceId)
        .timeout(Duration.ofSeconds(5))
        .flatMap(opt => ZIO.fromOption(opt).orElseFail(new RuntimeException("Error while getting space id")))
    }.orDie

    ((client ++ clock) >>> prepare) ++ client
  }
}
