package zio.tarantool.internal

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableArrayValueImpl
import _root_.zio._
import zio.tarantool.protocol._
import zio.tarantool.internal.schema.SchemaEncoder._
import zio.tarantool.internal.schema.{IndexMeta, SpaceMeta}
import zio.tarantool.TarantoolError.{IndexNotFound, SpaceNotFound}
import zio.tarantool.{TarantoolConfig, TarantoolError}

private[tarantool] object SchemaMetaManager {
  trait Service {
    def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta]

    def getIndexMeta(spaceName: String, indexName: String): IO[TarantoolError, IndexMeta]

    def refresh: IO[TarantoolError, Unit]

    def schemaId: UIO[Option[Long]]
  }

  def getSpaceMeta(spaceName: String): ZIO[Service, TarantoolError, SpaceMeta] =
    ZIO.serviceWithZIO(_.getSpaceMeta(spaceName))

  def getIndexMeta(
    spaceName: String,
    indexName: String
  ): ZIO[Service, TarantoolError, IndexMeta] =
    ZIO.serviceWithZIO(_.getIndexMeta(spaceName, indexName))

  def refresh(): ZIO[Service, TarantoolError, Unit] =
    ZIO.serviceWithZIO(_.refresh)

  def schemaId(): ZIO[Service, Nothing, Option[Long]] =
    ZIO.serviceWithZIO(_.schemaId)

  val live: ZLayer[
    TarantoolConfig with TarantoolConnection.Service with SyncIdProvider.Service with Clock,
    Nothing,
    Service
  ] =
    ZLayer {
      for {
        cfg <- ZIO.service[TarantoolConfig]
        connection <- ZIO.service[TarantoolConnection.Service]
        syncIdProvider <- ZIO.service[SyncIdProvider.Service]
        service <- make(cfg, connection, syncIdProvider)
      } yield service
    }

  val test: ZLayer[Any, Nothing, Service] =
    ZLayer.succeed(new Service {
      override def getSpaceMeta(spaceName: String): IO[TarantoolError, SpaceMeta] =
        ZIO.fail(TarantoolError.InternalError(new NotImplementedError()))

      override def getIndexMeta(
        spaceName: String,
        indexName: String
      ): IO[TarantoolError, IndexMeta] =
        ZIO.fail(TarantoolError.InternalError(new NotImplementedError()))

      override def refresh: IO[TarantoolError, Unit] =
        ZIO.fail(TarantoolError.InternalError(new NotImplementedError()))

      override def schemaId: UIO[Option[Long]] = ZIO.some(0)
    })

  // lazy start
  def make(
    cfg: TarantoolConfig,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ): URIO[Clock, Service] =
    for {
      clock <- ZIO.service[Clock]
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
        meta <- ZIO.ifZIO(ZIO.succeed(cache.contains(spaceName)))(
          ZIO.succeed(cache(spaceName)),
          ZIO.fail(SpaceNotFound(spaceName))
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
      ZIO
        .when(!space.indexes.contains(indexName))(ZIO.fail(IndexNotFound(spaceName, indexName)))
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
    ): IO[TarantoolError.Timeout, TarantoolResponse] =
      ZIO
        .withClock(clock) {
          select(spaceId, indexId).flatMap(
            _.response.await.timeout(cfg.clientConfig.schemaRequestTimeoutMillis.millis).flatMap(ZIO.fromOption(_))
          )
        }
        .orElseFail(
          TarantoolError.Timeout(s"Schema request timeout. SpaceId: $spaceId, indexId: $indexId")
        )

    private def select(
      spaceId: Int,
      indexId: Int
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        syncId <- syncIdProvider.syncId()
        body <- ZIO
          .attempt(
            TarantoolRequestBody.selectBody(spaceId, indexId, Int.MaxValue, Offset, IteratorCode.All, EmptyMpArray)
          )
          .mapError(TarantoolError.CodecError)
        request = TarantoolRequest(RequestCode.Select, syncId, body)
        operation <- connection.sendRequest(request)
      } yield operation

  }
}
