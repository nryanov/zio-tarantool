package zio.tarantool

import zio.{Has, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.tarantool.internal._
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.TarantoolContainer.Tarantool
import zio.tarantool.internal.RequestHandler.RequestHandler
import zio.tarantool.internal.ResponseHandler.ResponseHandler
import zio.tarantool.internal.SchemaMetaManager.SchemaMetaManager
import zio.tarantool.internal.SyncIdProvider.SyncIdProvider
import zio.tarantool.internal.TarantoolConnection.TarantoolConnection

trait BaseLayers {
  val tarantoolLayer: ZLayer[Any, Nothing, Tarantool] =
    Blocking.live >>> TarantoolContainer.tarantool()

  val tarantoolSecuredLayer: ZLayer[Any, Nothing, Tarantool] =
    Blocking.live >>> TarantoolSecuredContainer.tarantool()

  val configLayer: ZLayer[Any, Nothing, Has[TarantoolConfig]] =
    tarantoolLayer >>> ZLayer.fromService(container =>
      TarantoolConfig(
        host = container.container.getHost,
        port = container.container.getMappedPort(3301)
      )
    )

  val configSecuredLayer: ZLayer[Any, Nothing, Has[TarantoolConfig]] =
    tarantoolSecuredLayer >>> ZLayer.fromService(container =>
      TarantoolConfig(
        host = container.container.getHost,
        port = container.container.getMappedPort(3301),
        authInfo = AuthInfo("username", "password")
      )
    )

  val configNoMetaCacheLayer: ZLayer[Any, Nothing, Has[TarantoolConfig]] =
    tarantoolLayer >>> ZLayer.fromService(container =>
      TarantoolConfig(
        connectionConfig = ConnectionConfig(
          host = container.container.getHost,
          port = container.container.getMappedPort(3301)
        ),
        clientConfig = ClientConfig(useSchemaMetaCache = false),
        authInfo = None
      )
    )

  val syncIdProviderLayer: ZLayer[Any, Nothing, SyncIdProvider] = SyncIdProvider.live

  val requestHandlerLayer: ZLayer[Any, TarantoolError, RequestHandler] = RequestHandler.live

  val tarantoolConnectionLayer: ZLayer[Any, Throwable, TarantoolConnection] =
    (Clock.live ++ configLayer ++ syncIdProviderLayer ++ requestHandlerLayer) >>> TarantoolConnection.live

  val schemaMetaManagerLayer: ZLayer[Any, Throwable, SchemaMetaManager] =
    (configLayer ++ tarantoolConnectionLayer ++ syncIdProviderLayer ++ Clock.live) >>> SchemaMetaManager.live

  val responseHandlerLayer: ZLayer[Any, Throwable, ResponseHandler] =
    (tarantoolConnectionLayer ++ requestHandlerLayer) >>> ResponseHandler.live

  val tarantoolClientLayer: ZLayer[Any, Nothing, TarantoolClient] =
    ((Clock.live ++ configLayer) >>> TarantoolClient.live).orDie

  val tarantoolClientNotMetaCacheLayer: ZLayer[Any, Nothing, TarantoolClient] =
    ((Clock.live ++ configNoMetaCacheLayer) >>> TarantoolClient.live).orDie
}
