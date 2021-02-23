package zio.tarantool.internal.impl

import java.util.concurrent.ConcurrentHashMap

import zio.clock.Clock
import zio.tarantool.TarantoolError.{IndexNotFound, SpaceNotFound}
import zio.tarantool.internal.SchemaMetaManager
import zio.tarantool.internal.impl.SchemaMetaManagerLive._
import zio.tarantool.internal.schema.{IndexMeta, SpaceMeta}
import zio.tarantool.internal.schema.SchemaEncoder._
import zio.tarantool.msgpack.MpArray16
import zio.tarantool.protocol.IteratorCode
import zio.tarantool.{Logging, TarantoolClient, TarantoolConfig, TarantoolError}
import zio.{IO, Ref, Semaphore, UIO}

private[tarantool] class SchemaMetaManagerLive(
  cfg: TarantoolConfig,
  client: TarantoolClient.Service,
  clock: Clock,
  currentSchemaVersion: Ref[Long],
  fetchSemaphore: Semaphore
) extends SchemaMetaManager.Service
    with Logging {

  private val spaceMetaMap: ConcurrentHashMap[String, SpaceMeta] =
    new ConcurrentHashMap[String, SpaceMeta]()

  override def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta] =
    IO.ifM(IO.effectTotal(spaceMetaMap.contains(spaceName)))(
      IO.effectTotal(spaceMetaMap.get(spaceName)),
      IO.fail(SpaceNotFound(s"Space $spaceName not found in cache"))
    )

  override def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta] =
    for {
      space <- getSpaceMeta(spaceName)
      index <- IO.ifM(IO.effectTotal(space.indexes.contains(indexName)))(
        IO.effectTotal(space.indexes(spaceName)),
        IO.fail(IndexNotFound(s"Index $indexName not found in cache for space $spaceName"))
      )
    } yield index

  override def fetchMeta: IO[TarantoolError, Unit] =
    for {
      spacesOp <- client.select(
        VSpaceId,
        VSpaceIndexID,
        Int.MaxValue,
        0,
        IteratorCode.All,
        EmptyMpArray
      )
      indexesOp <- client.select(
        VIndexId,
        VIndexIdIndexId,
        Int.MaxValue,
        0,
        IteratorCode.All,
        EmptyMpArray
      )
      spaces <- spacesOp
        .data[SpaceMeta]
        .provide(clock)
        .mapError(err => TarantoolError.Timeout(err.getLocalizedMessage))
      indexes <- indexesOp
        .data[IndexMeta]
        .provide(clock)
        .mapError(err => TarantoolError.Timeout(err.getLocalizedMessage))
      _ <- info(s"Spaces: $spaces")
      _ <- info(s"Indexes: $indexes")
    } yield ()

  override def schemaVersion: UIO[Long] = currentSchemaVersion.get
}

private[tarantool] object SchemaMetaManagerLive {
  val VSpaceId = 281
  val VSpaceIndexID = 0

  val VIndexId = 289
  val VIndexIdIndexId = 0

  val EmptyMpArray = MpArray16(Vector.empty)
}
