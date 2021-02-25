package zio.tarantool.internal

import zio._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.internal.schema.{IndexMeta, SpaceMeta}
import zio.tarantool.{TarantoolConfig, TarantoolConnection, TarantoolError}
import zio.tarantool.internal.impl.SchemaMetaManagerLive

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
  ] with TarantoolConnection with Clock, Nothing, SchemaMetaManager] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      TarantoolConnection.Service,
      Clock,
      Nothing,
      Service
    ] { (cfg, client) =>
      make(cfg, client)
    }

  def make(
    cfg: TarantoolConfig,
    connection: TarantoolConnection.Service
  ): ZManaged[Clock, Nothing, Service] =
    ZManaged.fromEffect(
      for {
        clock <- ZIO.environment[Clock]
        currentSchemaVersion <- Ref.make(0L)
        spaceMetaMap <- Ref.make(Map.empty[String, SpaceMeta])
        semaphore <- Semaphore.make(1)
      } yield new SchemaMetaManagerLive(
        cfg,
        connection,
        clock,
        currentSchemaVersion,
        spaceMetaMap,
        semaphore
      )
    )
}
