package zio.tarantool.internal

import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.internal.schema.{IndexMeta, SpaceMeta}
import zio.tarantool.{TarantoolClient, TarantoolConfig, TarantoolError}
import zio._
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
  ] with TarantoolClient with Clock, Nothing, SchemaMetaManager] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      TarantoolClient.Service,
      Clock,
      Nothing,
      Service
    ] { (cfg, client) =>
      make(cfg, client)
    }

  def make(
    cfg: TarantoolConfig,
    client: TarantoolClient.Service
  ): ZManaged[Clock, Nothing, Service] =
    ZManaged.fromEffect(
      for {
        clock <- ZIO.environment[Clock]
        currentSchemaVersion <- Ref.make(0L)
        semaphore <- Semaphore.make(1)
      } yield new SchemaMetaManagerLive(
        cfg,
        client,
        clock,
        currentSchemaVersion,
        semaphore
      )
    )
}
