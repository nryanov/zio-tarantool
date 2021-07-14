//package zio.tarantool
//
//import java.time.Duration
// fixme
//import zio.test._
//import zio.clock.Clock
//import zio.test.Assertion._
//import zio.tarantool.codec.auto._
//import zio.tarantool.protocol.IteratorCode
//import zio.tarantool.codec.TupleOpsBuilder
//import zio.test.TestAspect.{after, sequential}
//import zio.tarantool.data.{TestTuple, TestTupleKey}
//
//object TarantoolClientWithSchemaSpec extends TarantoolBaseSpec {
//  private val tuple = TestTuple("key1", 1, 1)
//  private val key = TestTupleKey("key1")
//
//  private val insertAndSelect =
//    testM("insert and select record") {
//      for {
//        _ <- TarantoolClient.insert("test", tuple)
//        select <- TarantoolClient.select("test", "primary", 1, 0, IteratorCode.Eq, key)
//        result <- awaitResponseData[TestTuple](select)
//      } yield assert(result)(equalTo(Vector(tuple)))
//    }
//
//  private val update = testM("update record") {
//    for {
//      insert <- TarantoolClient.insert("test", tuple)
//      inserted <- awaitResponseData[TestTuple](insert)
//      ops <- TupleOpsBuilder[TestTuple].assign("f2", 2).assign("f3", 3).buildM()
//      _ <- TarantoolClient.update("test", "primary", key, ops)
//      selectAfterUpdate <- TarantoolClient.select("test", "primary", 1, 0, IteratorCode.Eq, key)
//      afterUpdate <- awaitResponseData[TestTuple](selectAfterUpdate)
//    } yield assert(inserted)(equalTo(Vector(tuple))) && assert(afterUpdate)(
//      equalTo(Vector(tuple.copy(f2 = 2, f3 = 3)))
//    )
//  }
//
//  private val delete = testM("delete record") {
//    for {
//      insert <- TarantoolClient.insert("test", tuple)
//      inserted <- awaitResponseHeadOption[TestTuple](insert)
//      _ <- TarantoolClient.delete("test", "primary", key)
//      selectDeleted <- TarantoolClient.select("test", "primary", 1, 0, IteratorCode.Eq, key)
//      none <- awaitResponseHeadOption[TestTuple](selectDeleted)
//    } yield assert(inserted)(isSome(equalTo(tuple))) && assert(none)(isNone)
//  }
//
//  private val upsert = testM("upsert record") {
//    for {
//      ops <- TupleOpsBuilder[TestTuple].assign("f2", 2).assign("f3", 3).buildM()
//      _ <- TarantoolClient.upsert("test", "primary", ops, tuple)
//      selectInserted <- TarantoolClient.select("test", "primary", 1, 0, IteratorCode.Eq, key)
//      inserted <- awaitResponseHeadOption[TestTuple](selectInserted)
//      _ <- TarantoolClient.upsert("test", "primary", ops, tuple)
//      selectUpdated <- TarantoolClient.select("test", "primary", 1, 0, IteratorCode.Eq, key)
//      updated <- awaitResponseHeadOption[TestTuple](selectUpdated)
//    } yield assert(inserted)(isSome(equalTo(tuple))) && assert(updated)(
//      isSome(equalTo(tuple.copy(f2 = 2, f3 = 3)))
//    )
//  }
//
//  private val replace = testM("replace record") {
//    for {
//      insert <- TarantoolClient.insert("test", tuple)
//      inserted <- awaitResponseHeadOption[TestTuple](insert)
//      replace <- TarantoolClient.replace("test", tuple.copy(f2 = 2, f3 = 3))
//      replaced <- awaitResponseHeadOption[TestTuple](replace)
//    } yield assert(inserted)(isSome(equalTo(tuple))) && assert(replaced)(
//      isSome(equalTo(tuple.copy(f2 = 2, f3 = 3)))
//    )
//  }
//
//  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
//    (suite("TarantoolClient with schema meta cache")(
//      insertAndSelect,
//      update,
//      delete,
//      upsert,
//      replace
//    ) @@ sequential @@ after(truncateSpace())).provideCustomLayerShared(createSharedLayer())
//
//  private def createSharedLayer() = {
//    val client = tarantoolClientLayer
//    val clock = Clock.live
//
//    val prepare = (for {
//      _ <- createSpace()
//      _ <- TarantoolClient.refreshMeta()
//    } yield ()).timeout(Duration.ofSeconds(5)).unit.toLayer.orDie
//
//    ((client ++ clock) >>> prepare) ++ client
//  }
//}
