package zio.tarantool.core

import zio._
import zio.logging._
import zio.duration._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.protocol._
import zio.tarantool.core.schema.SchemaEncoder._
import zio.tarantool.msgpack.MpArray16
import zio.tarantool.core.schema.{IndexMeta, SpaceMeta}
import zio.tarantool.TarantoolError.{IndexNotFound, SpaceNotFound}
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.SyncIdProvider.SyncIdProvider
import zio.tarantool.core.TarantoolConnection.TarantoolConnection
import zio.tarantool.{TarantoolConfig, TarantoolError}

@accessible[SchemaMetaManager.Service]
private[tarantool] object SchemaMetaManager {
  type SchemaMetaManager = Has[Service]

  trait Service {
    def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta]

    def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta]

    def refresh: IO[TarantoolError, Unit]

    def schemaId: UIO[Long]
  }

  val live: ZLayer[Has[
    TarantoolConfig
  ] with RequestHandler with TarantoolConnection with SyncIdProvider with Clock with Logging, Nothing, SchemaMetaManager] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      RequestHandler.Service,
      TarantoolConnection.Service,
      SyncIdProvider.Service,
      Clock with Logging,
      Nothing,
      Service
    ] { (cfg, requestHandler, connection, syncIdProvider) =>
      make(cfg, requestHandler, connection, syncIdProvider)
    }

  val test: ZLayer[Any, Nothing, SchemaMetaManager] =
    ZLayer.succeed(new Service {
      override def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta] =
        IO.fail(TarantoolError.InternalError(new NotImplementedError()))

      override def getIndexMeta(
        spaceName: String,
        indexName: String
      ): IO[TarantoolError, IndexMeta] =
        IO.fail(TarantoolError.InternalError(new NotImplementedError()))

      override def refresh: IO[TarantoolError, Unit] =
        IO.fail(TarantoolError.InternalError(new NotImplementedError()))

      override def schemaId: UIO[Long] = UIO.succeed(0)
    })

  // lazy start
  def make(
    cfg: TarantoolConfig,
    requestHandler: RequestHandler.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ): ZManaged[Logging with Clock, Nothing, Service] =
    ZManaged.fromEffect(
      for {
        clock <- ZIO.environment[Clock]
        logger <- ZIO.service[Logger[String]]
        spaceMetaMap <- Ref.make(Map.empty[String, SpaceMeta])
        currentSchemaId <- Ref.make(0L)
        semaphore <- Semaphore.make(1)
      } yield new Live(
        cfg,
        requestHandler,
        connection,
        syncIdProvider,
        spaceMetaMap,
        currentSchemaId,
        semaphore,
        logger,
        clock
      )
    )

  /* space id with list of spaces meta */
  private[this] val VSpaceId = 281
  private[this] val VSpaceIndexID = 0

  /* space id with list of indexes meta */
  private[this] val VIndexId = 289
  private[this] val VIndexIdIndexId = 0

  private[this] val EmptyMpArray: MpArray16 = MpArray16(Vector.empty)
  private[this] val Offset = 0

  private[tarantool] class Live(
    cfg: TarantoolConfig,
    requestHandler: RequestHandler.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service,
    spaceMetaMap: Ref[Map[String, SpaceMeta]],
    currentSchemaId: Ref[Long],
    fetchSemaphore: Semaphore,
    logger: Logger[String],
    clock: Clock
  ) extends Service {

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
        index <- getIndexMeta0(space, spaceName, indexName)
      } yield index

    private def getIndexMeta0(
      space: SpaceMeta,
      spaceName: String,
      indexName: String
    ): IO[TarantoolError, IndexMeta] =
      IO.when(!space.indexes.contains(indexName))(
        IO.fail(IndexNotFound(s"Index $indexName not found in cache for space $spaceName"))
      ).map(_ => space.indexes(indexName))

    override def refresh: IO[TarantoolError, Unit] =
      IO.when(cfg.clientConfig.useSchemaMetaCache)(
        fetchSemaphore.withPermit {
          fetchMeta0.retry(schedule).provide(clock)
        }
      )

    override def schemaId: UIO[Long] = currentSchemaId.get

    private def fetchMeta0: ZIO[Any, TarantoolError, Unit] =
      for {
        spacesOp <- selectMeta(VSpaceId, VSpaceIndexID)
        indexesOp <- selectMeta(VIndexId, VIndexIdIndexId)
        _ <- ZIO.when(spacesOp.schemaId != indexesOp.schemaId)(
          logger.debug("Not equal schema id of space and index meta responses") *> ZIO.fail(
            TarantoolError.NotEqualSchemaId(
              "Not equal schema id of space and index meta responses"
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
      _ <- currentSchemaId.set(schemaId)
    } yield ()

    // implicit dependency on ResponseHandler
    private def selectMeta(
      spaceId: Int,
      indexId: Int
    ): IO[TarantoolError.Timeout, TarantoolResponse] = select(
      spaceId,
      indexId
    ).tapError(err =>
      logger.error(s"Error happened while fetching meta: ${err.getLocalizedMessage}")
    ).flatMap(
      _.response.await
        .timeout(cfg.clientConfig.schemaRequestTimeoutMillis.milliseconds)
        .flatMap(ZIO.fromOption(_))
    ).tapError(_ => logger.error(s"Schema request timeout. SpaceId: $spaceId, indexId: $indexId"))
      .orElseFail(
        TarantoolError.Timeout(s"Schema request timeout. SpaceId: $spaceId, indexId: $indexId")
      )
      .provide(clock)

    private def select(
      spaceId: Int,
      indexId: Int
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        _ <- logger.debug(s"Schema select request space id: $spaceId")
        syncId <- syncIdProvider.syncId()
        body <- ZIO
          .effect(
            TarantoolRequestBody
              .selectBody(spaceId, indexId, Int.MaxValue, Offset, IteratorCode.All, EmptyMpArray)
          )
          .mapError(TarantoolError.CodecError)
        request = TarantoolRequest(RequestCode.Select, syncId, None, body)
        response <- requestHandler.submitRequest(request)
        packet <- TarantoolRequest.createPacket(request)
        _ <- connection.sendRequest(packet)
      } yield response

  }
}
