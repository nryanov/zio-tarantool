package zio.tarantool.internal

import java.time.Duration

import zio.{Has, ZIO, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.duration._
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.{
  TarantoolClient,
  TarantoolConfig,
  TarantoolConnection,
  TarantoolContainer,
  TarantoolError
}
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.TarantoolContainer.Tarantool
import zio.tarantool.internal.SchemaMetaManager.SchemaMetaManager
import zio.tarantool.internal.schema.SpaceMeta
import zio.test.{DefaultRunnableSpec, ZSpec, assert, assertM, suite, testM}
import zio.test.Assertion._
import zio.test.TestAspect._

object SchemaMetaManagerSpec extends DefaultRunnableSpec {
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
  val schemaManagerLayer: ZLayer[Any with Tarantool, Throwable, SchemaMetaManager] =
    (Clock.live ++ configLayer ++ tarantoolConnectionLayer) >>> SchemaMetaManager.live
  val testEnv: ZLayer[Any, Throwable, Clock with SchemaMetaManager with TarantoolClient] =
    Clock.live ++ (tarantoolLayer >>> (tarantoolClientLayer ++ schemaManagerLayer))

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
          initialSchemaId <- SchemaMetaManager.schemaVersion
          _ <- SchemaMetaManager.fetchMeta
          updatedSchemaId <- SchemaMetaManager.schemaVersion
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
