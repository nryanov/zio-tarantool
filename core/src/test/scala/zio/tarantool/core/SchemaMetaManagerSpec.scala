package zio.tarantool.core

import zio._
import zio.duration._
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.core.SchemaIdProvider.SchemaIdProvider
import zio.tarantool.{BaseLayers, TarantoolClient, TarantoolError}
import zio.tarantool.core.SchemaMetaManager.SchemaMetaManager
import zio.test.{DefaultRunnableSpec, ZSpec, assert, assertM, suite, testM}
import zio.test.Assertion._
import zio.test.TestAspect._

object SchemaMetaManagerSpec extends DefaultRunnableSpec with BaseLayers {
  val testEnv
    : ZLayer[Any, Throwable, SchemaIdProvider with TarantoolClient with SchemaMetaManager] =
    schemaIdProviderLayer ++ tarantoolClientLayer ++ schemaMetaManagerLayer

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    (suite("SchemaMetaManager spec")(
      testM("should fail if space not found in cache") {
        assertM(SchemaMetaManager.getSpaceMeta("some space").run)(
          fails(
            equalTo(
              TarantoolError.SpaceNotFound(s"Space some space not found in cache")
            )
          )
        )
      },
      testM("should update schema id after fetching meta") {
        for {
          initialSchemaId <- SchemaIdProvider.schemaId
          _ <- SchemaMetaManager.fetchMeta
          updatedSchemaId <- SchemaIdProvider.schemaId
        } yield assert(initialSchemaId)(equalTo(0L)) && assert(updatedSchemaId)(not(equalTo(0L)))
      },
      testM("should fetch spaces and indexes") {
        for {
          _ <- createSpace()
          _ <- SchemaMetaManager.fetchMeta
          space <- SchemaMetaManager.getSpaceMeta("test")
          index <- SchemaMetaManager.getIndexMeta("test", "primary")
        } yield assert(space.spaceName)(equalTo("test")) && assert(index.indexName)(
          equalTo("primary")
        )
      },
      testM("should fail if index not found in cache") {
        val result = for {
          _ <- createSpace()
          _ <- SchemaMetaManager.fetchMeta
          _ <- SchemaMetaManager.getIndexMeta("test", "notExistingIndex")
        } yield ()

        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.IndexNotFound(
                "Index notExistingIndex not found in cache for space test"
              )
            )
          )
        )
      }
    ) @@ sequential @@ timeout(30 seconds)).provideCustomLayerShared(testEnv.orDie)

  private def createSpace(): ZIO[Any with TarantoolClient, Throwable, Unit] = for {
    r1 <- TarantoolClient.eval("box.schema.create_space('test', {if_not_exists = true})")
    r2 <- TarantoolClient.eval(
      "box.space.test:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'string'} })"
    )
    r3 <- TarantoolClient.eval(
      "box.space.test:create_index('secondary', {if_not_exists = true, unique = false, parts = {2, 'string'} })"
    )
    _ <- r1.response.await
    _ <- r2.response.await
    _ <- r3.response.await
  } yield ()
}
