package zio.tarantool.schema

import _root_.zio.test._
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect.{after, sequential}
import zio.tarantool.codec.auto._
import zio.tarantool.schema.IndexPart.ByPosition
import zio.tarantool.{TarantoolBaseSpec, TarantoolClient}

object SchemaApiSpec extends TarantoolBaseSpec {
  private val spaceName = "schema_api_space"

  private val createAndUseByName =
    test("create space and index then use name-based CRUD without manual refresh") {
      for {
        _ <- TarantoolClient.schema
          .createSpace(spaceName)
          .ifNotExists(true)
          .run
        _ <- TarantoolClient.schema
          .createIndex(spaceName, "primary")
          .unique(true)
          .ifNotExists(true)
          .parts(ByPosition(1, "string"))
          .run
        meta <- TarantoolClient.schema.spaceMeta(spaceName)
        index <- TarantoolClient.schema.indexMeta(spaceName, "primary")
        _ <- TarantoolClient.insert.into(spaceName).tuple(("key1", 1)).run
        select <- TarantoolClient.select
          .from(spaceName)
          .index("primary")
          .key(Tuple1("key1"))
          .limit(1)
          .run
        result <- awaitResponseData[(String, Int)](select)
      } yield assertTrue(meta.spaceName == spaceName) &&
        assertTrue(index.indexName == "primary") &&
        assert(result)(equalTo(Vector(("key1", 1))))
    }

  private val truncateSpaceTest =
    test("truncate removes tuples") {
      for {
        _ <- TarantoolClient.schema.createSpace(spaceName).ifNotExists(true).run
        _ <- TarantoolClient.schema
          .createIndex(spaceName, "primary")
          .unique(true)
          .ifNotExists(true)
          .parts(ByPosition(1, "string"))
          .run
        _ <- TarantoolClient.insert.into(spaceName).tuple(("key1", 1)).run
        _ <- TarantoolClient.schema.truncate(spaceName).run
        select <- TarantoolClient.select
          .from(spaceName)
          .index("primary")
          .key(Tuple1("key1"))
          .limit(1)
          .run
        result <- awaitResponseHeadOption[(String, Int)](select)
      } yield assert(result)(isNone)
    }

  private val dropSpaceAndIndex =
    test("drop index and space") {
      for {
        _ <- TarantoolClient.schema.createSpace(spaceName).ifNotExists(true).run
        _ <- TarantoolClient.schema
          .createIndex(spaceName, "primary")
          .unique(true)
          .ifNotExists(true)
          .parts(ByPosition(1, "string"))
          .run
        _ <- TarantoolClient.schema
          .createIndex(spaceName, "secondary")
          .unique(false)
          .ifNotExists(true)
          .parts(ByPosition(2, "unsigned"))
          .run
        _ <- TarantoolClient.schema.dropIndex(spaceName, "secondary").run
        afterDropIndex <- TarantoolClient.schema.spaceMeta(spaceName)
        _ <- TarantoolClient.schema.dropSpace(spaceName).run
        missing <- TarantoolClient.schema.spaceMeta(spaceName).either
      } yield assertTrue(!afterDropIndex.indexes.contains("secondary")) &&
        assertTrue(missing.isLeft)
    }

  override def spec: Spec[TestEnvironment, Any] =
    (suite("SchemaApi")(
      createAndUseByName,
      truncateSpaceTest,
      dropSpaceAndIndex
    ) @@ sequential @@ after(
      TarantoolClient.schema.dropSpace(spaceName).run.ignore
    )).provideLayerShared(tarantoolClientLayer)
}
