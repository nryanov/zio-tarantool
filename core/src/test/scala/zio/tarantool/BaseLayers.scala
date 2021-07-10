package zio.tarantool

import zio.{Has, ZLayer}
import zio.console.Console
import zio.blocking.Blocking
import zio.clock.Clock
import zio.logging.{LogLevel, Logging}
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

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Clock.live ++ Console.live) >>> Logging.console(logLevel = LogLevel.Debug)

  val syncIdProviderLayer: ZLayer[Any, Nothing, SyncIdProvider] = SyncIdProvider.live

  val requestHandlerLayer: ZLayer[Any, TarantoolError, RequestHandler] = RequestHandler.live

  val tarantoolConnectionLayer: ZLayer[Any, Throwable, TarantoolConnection] =
    (Clock.live ++ configLayer ++ loggingLayer ++ syncIdProviderLayer ++ requestHandlerLayer) >>> TarantoolConnection.live

  val schemaMetaManagerLayer: ZLayer[Any, Throwable, SchemaMetaManager] =
    (configLayer ++ tarantoolConnectionLayer ++ syncIdProviderLayer ++ Clock.live ++ loggingLayer) >>> SchemaMetaManager.live

  val responseHandlerLayer: ZLayer[Any, Throwable, ResponseHandler] =
    (tarantoolConnectionLayer ++ requestHandlerLayer ++ loggingLayer) >>> ResponseHandler.live

  val tarantoolClientLayer: ZLayer[Any, Nothing, TarantoolClient] =
    ((loggingLayer ++ Clock.live ++ configLayer) >>> TarantoolClient.live).orDie

  val tarantoolClientNotMetaCacheLayer: ZLayer[Any, Nothing, TarantoolClient] =
    ((loggingLayer ++ Clock.live ++ configNoMetaCacheLayer) >>> TarantoolClient.live).orDie
}
