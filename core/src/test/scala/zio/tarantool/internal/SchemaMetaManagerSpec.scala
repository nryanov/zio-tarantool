package zio.tarantool.internal

import zio.duration._
import zio.tarantool.{BaseLayers, TarantoolError}
import zio.test.{DefaultRunnableSpec, ZSpec, assert, assertM}
import zio.test.Assertion._
import zio.test.TestAspect._

object SchemaMetaManagerSpec extends DefaultRunnableSpec with BaseLayers {
  private val layer = responseHandlerLayer ++ schemaMetaManagerLayer

  private val failIfSpaceNotFound = testM("should fail if space not found in cache") {
    assertM(SchemaMetaManager.getSpaceMeta("some space").run)(
      fails(equalTo(TarantoolError.SpaceNotFound("some space")))
    )
  }

  private val fetchSpaceAndIndex = testM("should fetch spaces and indexes") {
    for {
      _ <- SchemaMetaManager.refresh()
      space <- SchemaMetaManager.getSpaceMeta("_vspace")
      index <- SchemaMetaManager.getIndexMeta("_vspace", "primary")
    } yield assert(space.spaceName)(equalTo("_vspace")) && assert(index.indexName)(
      equalTo("primary")
    )
  }

  private val failIfIndexNotFound = testM("should fail if index not found in cache") {
    val result = for {
      _ <- SchemaMetaManager.refresh()
      _ <- SchemaMetaManager.getIndexMeta("_vspace", "notExistingIndex")
    } yield ()

    assertM(result.run)(
      fails(equalTo(TarantoolError.IndexNotFound("_vspace", "notExistingIndex")))
    )
  }

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    (suite("SchemaMetaManager spec")(
      failIfSpaceNotFound.provideLayer(layer.orDie),
      fetchSpaceAndIndex.provideLayer(layer.orDie),
      failIfIndexNotFound.provideLayer(layer.orDie)
    ) @@ sequential @@ timeout(1 minute)).provideCustomLayerShared(tarantoolLayer)
}
