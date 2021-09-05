package zio.tarantool.internal

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableArrayValueImpl
import zio._
import zio.duration._
import zio.clock.Clock
import zio.tarantool.protocol._
import zio.tarantool.internal.schema.SchemaEncoder._
import zio.tarantool.internal.schema.{IndexMeta, SpaceMeta}
import zio.tarantool.TarantoolError.{IndexNotFound, SpaceNotFound}
import zio.tarantool.internal.SyncIdProvider.SyncIdProvider
import zio.tarantool.internal.TarantoolConnection.TarantoolConnection
import zio.tarantool.{TarantoolConfig, TarantoolError}

private[tarantool] object SchemaMetaManager {
  type SchemaMetaManager = Has[Service]

  trait Service {
    def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta]

    def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta]

    def refresh: IO[TarantoolError, Unit]

    def schemaId: UIO[Option[Long]]
  }

  def getSpaceMeta(spaceName: String): ZIO[SchemaMetaManager, TarantoolError, SpaceMeta] =
    ZIO.accessM[SchemaMetaManager](_.get.getSpaceMeta(spaceName))

  def getIndexMeta(
    spaceName: String,
    indexName: String
  ): ZIO[SchemaMetaManager, TarantoolError, IndexMeta] =
    ZIO.accessM[SchemaMetaManager](_.get.getIndexMeta(spaceName, indexName))

  def refresh(): ZIO[SchemaMetaManager, TarantoolError, Unit] =
    ZIO.accessM[SchemaMetaManager](_.get.refresh)

  def schemaId(): ZIO[SchemaMetaManager, Nothing, Option[Long]] =
    ZIO.accessM[SchemaMetaManager](_.get.schemaId)

  val live: ZLayer[Has[
    TarantoolConfig
  ] with TarantoolConnection with SyncIdProvider with Clock, Nothing, SchemaMetaManager] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      TarantoolConnection.Service,
      SyncIdProvider.Service,
      Clock,
      Nothing,
      Service
    ] { (cfg, connection, syncIdProvider) =>
      make(cfg, connection, syncIdProvider)
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

      override def schemaId: UIO[Option[Long]] = UIO.some(0)
    })

  // lazy start
  def make(
    cfg: TarantoolConfig,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ): ZManaged[Clock, Nothing, Service] =
    ZManaged.fromEffect(
      for {
        clock <- ZIO.environment[Clock]
        spaceMetaMap <- Ref.make(Map.empty[String, SpaceMeta])
        currentSchemaId <- Ref.make[Option[Long]](None)
        semaphore <- Semaphore.make(1)
      } yield new Live(
        cfg,
        connection,
        syncIdProvider,
        spaceMetaMap,
        currentSchemaId,
        semaphore,
        clock
      )
    )

  /* space id with list of spaces meta */
  private[this] val VSpaceId = 281
  private[this] val VSpaceIndexID = 0

  /* space id with list of indexes meta */
  private[this] val VIndexId = 289
  private[this] val VIndexIdIndexId = 0

  private[this] val EmptyMpArray: Value = new ImmutableArrayValueImpl(Array.empty)
  private[this] val Offset = 0

  private[tarantool] class Live(
    cfg: TarantoolConfig,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service,
    spaceMetaMap: Ref[Map[String, SpaceMeta]],
    currentSchemaId: Ref[Option[Long]],
    fetchSemaphore: Semaphore,
    clock: Clock
  ) extends Service {

    override def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta] =
      for {
        cache <- spaceMetaMap.get
        meta <- IO.ifM(IO.effectTotal(cache.contains(spaceName)))(
          IO.effectTotal(cache(spaceName)),
          IO.fail(SpaceNotFound(spaceName))
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
      IO.when(!space.indexes.contains(indexName))(IO.fail(IndexNotFound(spaceName, indexName)))
        .as(space.indexes(indexName))

    override def refresh: IO[TarantoolError, Unit] =
      fetchSemaphore.withPermit(fetchMeta0)

    override def schemaId: UIO[Option[Long]] = currentSchemaId.get

    private def fetchMeta0: ZIO[Any, TarantoolError, Unit] =
      for {
        spacesOpFiber <- selectMeta(VSpaceId, VSpaceIndexID).fork
        indexesOpFiber <- selectMeta(VIndexId, VIndexIdIndexId).fork
        spacesOp <- spacesOpFiber.join
        indexesOp <- indexesOpFiber.join
        _ <- updateMetaCache(spacesOp, indexesOp)
      } yield ()

    private def updateMetaCache(
      spacesOp: TarantoolResponse,
      indexesOp: TarantoolResponse
    ) = for {
      spaces <- spacesOp.resultSet[SpaceMeta]
      indexes <- indexesOp.resultSet[IndexMeta]
      groupedIndexes = indexes.groupBy(_.spaceId)
      mappedSpaceMeta = spaces.map(meta => meta.spaceId -> meta).toMap.map { case (spaceId, spaceMeta) =>
        spaceMeta.spaceName -> spaceMeta.withIndexes(
          groupedIndexes.getOrElse(spaceId, Vector.empty).map(indexMeta => indexMeta.indexName -> indexMeta).toMap
        )
      }
      _ <- spaceMetaMap.set(mappedSpaceMeta)
    } yield ()

    // implicit dependency on ResponseHandler
    private def selectMeta(
      spaceId: Int,
      indexId: Int
    ): IO[TarantoolError.Timeout, TarantoolResponse] = select(
      spaceId,
      indexId
    ).flatMap(
      _.response.await.timeout(cfg.clientConfig.schemaRequestTimeoutMillis.milliseconds).flatMap(ZIO.fromOption(_))
    ).orElseFail(
      TarantoolError.Timeout(s"Schema request timeout. SpaceId: $spaceId, indexId: $indexId")
    ).provide(clock)

    private def select(
      spaceId: Int,
      indexId: Int
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        syncId <- syncIdProvider.syncId()
        body <- ZIO
          .effect(
            TarantoolRequestBody.selectBody(spaceId, indexId, Int.MaxValue, Offset, IteratorCode.All, EmptyMpArray)
          )
          .mapError(TarantoolError.CodecError)
        request = TarantoolRequest(RequestCode.Select, syncId, body)
        operation <- connection.sendRequest(request)
      } yield operation

  }
}
