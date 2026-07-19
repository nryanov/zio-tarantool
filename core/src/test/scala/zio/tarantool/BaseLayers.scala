package zio.tarantool

import com.dimafeng.testcontainers.GenericContainer
import _root_.zio.{ZIO, ZLayer}
import _root_.zio.Clock
import zio.tarantool.internal._

trait BaseLayers {
  val tarantoolLayer: ZLayer[Any, Nothing, GenericContainer] =
    TarantoolContainer.tarantool()

  val tarantoolSecuredLayer: ZLayer[Any, Nothing, GenericContainer] =
    TarantoolSecuredContainer.tarantool()

  val configLayer: ZLayer[Any, Nothing, TarantoolConfig] =
    tarantoolLayer >>> ZLayer {
      ZIO.serviceWith[GenericContainer] { container =>
        TarantoolConfig(
          host = container.container.getHost,
          port = container.container.getMappedPort(3301)
        )
      }
    }

  val configSecuredLayer: ZLayer[Any, Nothing, TarantoolConfig] =
    tarantoolSecuredLayer >>> ZLayer {
      ZIO.serviceWith[GenericContainer] { container =>
        TarantoolConfig(
          host = container.container.getHost,
          port = container.container.getMappedPort(3301),
          authInfo = AuthInfo("username", "password")
        )
      }
    }

  val configNoMetaCacheLayer: ZLayer[Any, Nothing, TarantoolConfig] =
    tarantoolLayer >>> ZLayer {
      ZIO.serviceWith[GenericContainer] { container =>
        TarantoolConfig(
          connectionConfig = ConnectionConfig(
            host = container.container.getHost,
            port = container.container.getMappedPort(3301)
          ),
          clientConfig = ClientConfig(useSchemaMetaCache = false),
          authInfo = None
        )
      }
    }

  val syncIdProviderLayer: ZLayer[Any, Nothing, SyncIdProvider.Service] = SyncIdProvider.live

  val requestHandlerLayer: ZLayer[Any, Nothing, RequestHandler.Service] = RequestHandler.live

  val tarantoolConnectionLayer: ZLayer[Any, Throwable, TarantoolConnection.Service] =
    (ZLayer.succeed[Clock](
      Clock.ClockLive
    ) ++ configLayer ++ syncIdProviderLayer ++ requestHandlerLayer) >>> TarantoolConnection.live

  val schemaMetaManagerLayer: ZLayer[Any, Throwable, SchemaMetaManager.Service] =
    (configLayer ++ tarantoolConnectionLayer ++ syncIdProviderLayer ++ ZLayer.succeed[Clock](
      Clock.ClockLive
    )) >>> SchemaMetaManager.live

  val responseHandlerLayer: ZLayer[Any, Throwable, ResponseHandler.Service] =
    (tarantoolConnectionLayer ++ requestHandlerLayer) >>> ResponseHandler.live

  val tarantoolClientLayer: ZLayer[Any, Nothing, TarantoolClient.Service] =
    ((ZLayer.succeed[Clock](Clock.ClockLive) ++ configLayer) >>> TarantoolClient.live).orDie

  val tarantoolClientNotMetaCacheLayer: ZLayer[Any, Nothing, TarantoolClient.Service] =
    ((ZLayer.succeed[Clock](Clock.ClockLive) ++ configNoMetaCacheLayer) >>> TarantoolClient.live).orDie
}
