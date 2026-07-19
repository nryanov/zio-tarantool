package zio.tarantool.internal

import _root_.zio.durationInt
import zio.tarantool.{BaseLayers, TarantoolError}
import _root_.zio.test._
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect._

object SchemaMetaManagerSpec extends ZIOSpecDefault with BaseLayers {
  private val layer = responseHandlerLayer ++ schemaMetaManagerLayer

  private val failIfSpaceNotFound = test("should fail if space not found in cache") {
    assertZIO(SchemaMetaManager.getSpaceMeta("some space").exit)(
      fails(equalTo(TarantoolError.SpaceNotFound("some space")))
    )
  }

  private val fetchSpaceAndIndex = test("should fetch spaces and indexes") {
    for {
      _ <- SchemaMetaManager.refresh()
      space <- SchemaMetaManager.getSpaceMeta("_vspace")
      index <- SchemaMetaManager.getIndexMeta("_vspace", "primary")
    } yield assert(space.spaceName)(equalTo("_vspace")) && assert(index.indexName)(
      equalTo("primary")
    )
  }

  private val failIfIndexNotFound = test("should fail if index not found in cache") {
    val result = for {
      _ <- SchemaMetaManager.refresh()
      _ <- SchemaMetaManager.getIndexMeta("_vspace", "notExistingIndex")
    } yield ()

    assertZIO(result.exit)(
      fails(equalTo(TarantoolError.IndexNotFound("_vspace", "notExistingIndex")))
    )
  }

  override def spec: Spec[TestEnvironment, Any] =
    (suite("SchemaMetaManager spec")(
      failIfSpaceNotFound.provideLayer(layer.orDie),
      fetchSpaceAndIndex.provideLayer(layer.orDie),
      failIfIndexNotFound.provideLayer(layer.orDie)
    ) @@ sequential @@ timeout(1.minute)).provideLayerShared(tarantoolLayer)
}
