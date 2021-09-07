package zio.tarantool

import java.time.Duration

import zio._
import zio.clock.Clock
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.tarantool.codec.auto._
import zio.tarantool.codec.TupleOpsBuilder
import zio.tarantool.data.{TestTuple, TestTupleKey}
import zio.tarantool.data.TestTuple._
import zio.tarantool.protocol.IteratorCode

object TarantoolClientSpec extends TarantoolBaseSpec {
  private val tuple = TestTuple("key1", 1, 1)
  private val key = TestTupleKey("key1")

  final case class SumFunctionArgs(a: Int, b: Int)

  private val eval = testM("eval") {
    for {
      operation <- TarantoolClient.eval("return 123")
      result <- awaitResponse(operation).flatMap(_.head[Int])
    } yield assert(result)(equalTo(123))
  }

  private val call = testM("call") {
    for {
      operation <- TarantoolClient.call("sum", SumFunctionArgs(2, 5))
      result <- awaitResponse(operation).flatMap(_.head[Int])
    } yield assert(result)(equalTo(7))
  }

  private val insert = testM("insert") {
    for {
      spaceId <- ZIO.service[Int]
      _ <- TarantoolClient.insert(spaceId, tuple)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      result <- awaitResponseData[TestTuple](operation)
    } yield assert(result)(equalTo(Vector(tuple)))
  }

  private val upsert = testM("upsert") {
    for {
      spaceId <- ZIO.service[Int]
      ops <- TupleOpsBuilder[TestTuple].assign("f2", 321).assign("f3", 11).buildM()
      _ <- TarantoolClient.upsert(spaceId, 0, ops, tuple)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      initialValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.upsert(spaceId, 0, ops, tuple)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      updatedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(initialValue)(equalTo(Vector(tuple))) &&
      assert(updatedValue)(equalTo(Vector(tuple.copy(f2 = 321, f3 = 11))))
  }

  private val delete = testM("delete") {
    for {
      spaceId <- ZIO.service[Int]
      _ <- TarantoolClient.insert(spaceId, tuple)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      insertedValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.delete(spaceId, 0, key)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      deletedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(insertedValue)(equalTo(Vector(tuple))) &&
      assert(deletedValue)(equalTo(Vector.empty))
  }

  private val update = testM("update") {
    for {
      spaceId <- ZIO.service[Int]
      ops <- TupleOpsBuilder[TestTuple].assign("f2", 123).assign("f3", 321).buildM()
      _ <- TarantoolClient.insert(spaceId, tuple)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      initialValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.update(spaceId, 0, key, ops)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      updatedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(initialValue)(equalTo(Vector(tuple))) &&
      assert(updatedValue)(equalTo(Vector(tuple.copy(f2 = 123, f3 = 321))))
  }

  private val replace = testM("replace") {
    for {
      spaceId <- ZIO.service[Int]
      _ <- TarantoolClient.insert(spaceId, tuple)
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      initialValue <- awaitResponseData[TestTuple](operation)
      _ <- TarantoolClient.replace(spaceId, tuple.copy(f2 = 12345))
      operation <- TarantoolClient.select(spaceId, 0, 1, 0, IteratorCode.Eq, key)
      replacedValue <- awaitResponseData[TestTuple](operation)
    } yield assert(initialValue)(equalTo(Vector(tuple))) && assert(replacedValue)(
      equalTo(Vector(tuple.copy(f2 = 12345)))
    )
  }

  private val executeSqlStatement = testM("execute sql statement") {
    for {
      query1 <- TarantoolClient.execute(
        "CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))"
      )
      query2 <- TarantoolClient.execute("INSERT INTO table1 VALUES (1, 'A')")
      query3 <- TarantoolClient.execute("SELECT * FROM table1 WHERE column1 = 1")
      _ <- awaitResponse(query1)
      _ <- awaitResponse(query2)
      _ <- awaitResponse(query3)
    } yield assert(true)(isTrue)
  }

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    (suite("TarantoolClient without schema meta cache")(
      eval,
      call,
      insert,
      delete,
      upsert,
      update,
      replace,
      executeSqlStatement
    ) @@ sequential @@ after(truncateSpace())).provideCustomLayerShared(createSharedLayer())

  private def createSharedLayer() = {
    val client = tarantoolClientLayer
    val clock = Clock.live

    val prepare = (for {
      _ <- createSpace()
      _ <- createFunction()
      spaceId <- getSpaceId()
    } yield spaceId)
      .timeout(Duration.ofSeconds(5))
      .flatMap(opt => ZIO.require(new RuntimeException("Error while getting space id"))(ZIO.succeed(opt)))
      .toLayer
      .orDie

    ((client ++ clock) >>> prepare) ++ client
  }
}
