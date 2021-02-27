package zio.tarantool.core

import zio._
import zio.duration._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.protocol._
import zio.tarantool.core.schema.SchemaEncoder._
import zio.tarantool.msgpack.{MpArray, MpArray16}
import zio.tarantool.core.schema.{IndexMeta, SpaceMeta}
import TarantoolConnection.TarantoolConnection
import zio.logging.Logger
import zio.tarantool.TarantoolError.{IndexNotFound, SpaceNotFound}
import zio.tarantool.{Logging, TarantoolConfig, TarantoolError}

@accessible
private[tarantool] object SchemaMetaManager {
  type SchemaMetaManager = Has[Service]

  trait Service {
    def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta]

    def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta]

    def fetchMeta: IO[TarantoolError, Unit]

    def schemaVersion: UIO[Long]
  }

  val live: ZLayer[Has[
    TarantoolConfig
  ] with TarantoolConnection with Clock, Nothing, SchemaMetaManager] = ???
//    ZLayer.fromServicesManaged[
//      TarantoolConfig,
//      TarantoolConnection.Service,
//      Clock,
//      Nothing,
//      Service
//    ] { (cfg, client) =>
//      make(cfg, client)
//    }

//  def make(
//    cfg: TarantoolConfig,
//    connection: TarantoolConnection.Service
//  ): ZManaged[Clock, Nothing, Service] =
//    ZManaged.fromEffect(
//      for {
//        clock <- ZIO.environment[Clock]
//        currentSchemaVersion <- Ref.make(0L)
//        spaceMetaMap <- Ref.make(Map.empty[String, SpaceMeta])
//        semaphore <- Semaphore.make(1)
//      } yield new Live(
//        cfg,
//        connection,
//        clock,
//        currentSchemaVersion,
//        spaceMetaMap,
//        semaphore
//      )
//    )

  private[this] val VSpaceId = 281
  private[this] val VSpaceIndexID = 0

  private[this] val VIndexId = 289
  private[this] val VIndexIdIndexId = 0

  private[this] val EmptyMpArray: MpArray16 = MpArray16(Vector.empty)

  private[this] class Live(
    logger: Logger[String],
    cfg: TarantoolConfig,
    interceptor: TarantoolCommunicationInterceptor.Service,
    clock: Clock,
    currentSchemaVersion: Ref[Long],
    spaceMetaMap: Ref[Map[String, SpaceMeta]],
    fetchSemaphore: Semaphore
  ) extends SchemaMetaManager.Service {

    private val schedule: Schedule[Any, TarantoolError, Unit] =
      (Schedule.recurs(cfg.clientConfig.schemaRequestRetries) && Schedule.spaced(
        cfg.clientConfig.schemaRequestRetryTimeoutMillis.milliseconds
      ) && Schedule.recurWhile[TarantoolError] {
        case _: TarantoolError.NotEqualSchemaId => true
        case _                                  => false
      }).unit

    override def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta] =
      for {
        cache <- spaceMetaMap.get
        meta <- IO.ifM(IO.effectTotal(cache.contains(spaceName)))(
          IO.effectTotal(cache(spaceName)),
          IO.fail(SpaceNotFound(s"Space $spaceName not found in cache"))
        )
      } yield meta

    override def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta] =
      for {
        space <- getSpaceMeta(spaceName)
        index <- IO.ifM(IO.effectTotal(space.indexes.contains(indexName)))(
          IO.effectTotal(space.indexes(indexName)),
          IO.fail(IndexNotFound(s"Index $indexName not found in cache for space $spaceName"))
        )
      } yield index

    override def fetchMeta: IO[TarantoolError, Unit] = fetchSemaphore.withPermit {
      fetchMeta0.retry(schedule).provide(clock)
    }

    private def fetchMeta0: ZIO[Any, TarantoolError, Unit] =
      for {
        spacesOp <- selectMeta(VSpaceId, VSpaceIndexID)
        indexesOp <- selectMeta(VIndexId, VIndexIdIndexId)
        _ <- ZIO.when(spacesOp.schemaId != indexesOp.schemaId)(
          ZIO.fail(
            TarantoolError.NotEqualSchemaId(
              "Not equals schema id of space and index meta responses"
            )
          )
        )
        _ <- updateMetaCache(spacesOp.schemaId, spacesOp, indexesOp)
      } yield ()

    private def updateMetaCache(
      schemaId: Long,
      spacesOp: TarantoolResponse,
      indexesOp: TarantoolResponse
    ) = for {
      spaces <- spacesOp.dataUnsafe[SpaceMeta].mapError(TarantoolError.CodecError)
      indexes <- indexesOp.dataUnsafe[IndexMeta].mapError(TarantoolError.CodecError)
      groupedIndexes = indexes.groupBy(_.spaceId)
      mappedSpaceMeta = spaces.map(meta => meta.spaceId -> meta).toMap.map {
        case (spaceId, spaceMeta) =>
          spaceMeta.spaceName -> spaceMeta.withIndexes(
            groupedIndexes
              .getOrElse(spaceId, Vector.empty)
              .map(indexMeta => indexMeta.indexName -> indexMeta)
              .toMap
          )
      }
      _ <- spaceMetaMap.set(mappedSpaceMeta)
      _ <- currentSchemaVersion.set(schemaId)
    } yield ()

    override def schemaVersion: UIO[Long] = currentSchemaVersion.get

    private def selectMeta(spaceId: Int, indexId: Int) = select(
      spaceId,
      indexId,
      Int.MaxValue,
      0,
      IteratorCode.All,
      EmptyMpArray
    ).flatMap(
      _.response.await.timeout(cfg.clientConfig.schemaRequestTimeoutMillis.milliseconds).flatMap {
        v =>
          ZIO.fromOption(v).tapError(_ => logger.error("Schema request timeout"))
      }
    ).orElseFail(TarantoolError.Timeout("Schema request timeout"))
      .provide(clock)

    private def select(
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        _ <- logger.debug(s"Schema select request: $key")
        body <- ZIO
          .effect(TarantoolRequestBody.selectBody(spaceId, indexId, limit, offset, iterator, key))
          .mapError(TarantoolError.CodecError)
        response <- interceptor.submitRequest(OperationCode.Select, body)
      } yield response

  }
}
