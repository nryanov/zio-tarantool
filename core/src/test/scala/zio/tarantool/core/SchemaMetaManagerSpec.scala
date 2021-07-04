package zio.tarantool.core

import zio._
import zio.duration._
import zio.tarantool.core.ResponseHandler.ResponseHandler
import zio.tarantool.{BaseLayers, TarantoolError}
import zio.tarantool.core.SchemaMetaManager.SchemaMetaManager
import zio.test.{DefaultRunnableSpec, ZSpec, assert, assertM, suite, testM}
import zio.test.Assertion._
import zio.test.TestAspect._

object SchemaMetaManagerSpec extends DefaultRunnableSpec with BaseLayers {
  val testEnv: ZLayer[Any, Throwable, ResponseHandler with SchemaMetaManager] =
    responseHandlerLayer ++ schemaMetaManagerLayer

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
      testM("should fetch spaces and indexes") {
        for {
          _ <- SchemaMetaManager.refresh
          space <- SchemaMetaManager.getSpaceMeta("_vspace")
          index <- SchemaMetaManager.getIndexMeta("_vspace", "primary")
        } yield assert(space.spaceName)(equalTo("_vspace")) && assert(index.indexName)(
          equalTo("primary")
        )
      },
      testM("should fail if index not found in cache") {
        val result = for {
          _ <- SchemaMetaManager.refresh
          _ <- SchemaMetaManager.getIndexMeta("_vspace", "notExistingIndex")
        } yield ()

        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.IndexNotFound(
                "Index notExistingIndex not found in cache for space _vspace"
              )
            )
          )
        )
      }
    ) @@ sequential @@ timeout(30 seconds)).provideCustomLayerShared(testEnv.orDie)
}
