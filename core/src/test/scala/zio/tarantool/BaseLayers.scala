package zio.tarantool

import zio.{Has, ZLayer}
import zio.console.Console
import zio.blocking.Blocking
import zio.clock.Clock
import zio.logging.{LogLevel, Logging}
import zio.tarantool.core._
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.TarantoolContainer.Tarantool
import zio.tarantool.core.CommunicationFacade.CommunicationFacade
import zio.tarantool.core.DelayedQueue.DelayedQueue
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.ResponseHandler.ResponseHandler
import zio.tarantool.core.SchemaMetaManager.SchemaMetaManager
import zio.tarantool.core.SocketChannelQueuedWriter.SocketChannelQueuedWriter
import zio.tarantool.core.SyncIdProvider.SyncIdProvider
import zio.tarantool.core.TarantoolConnection.TarantoolConnection

trait BaseLayers {
  val tarantoolLayer: ZLayer[Any, Nothing, Tarantool] =
    Blocking.live >>> TarantoolContainer.tarantool()

  val configLayer: ZLayer[Any, Nothing, Has[TarantoolConfig]] =
    tarantoolLayer >>> ZLayer.fromService(container =>
      TarantoolConfig(
        host = container.container.getHost,
        port = container.container.getMappedPort(3301)
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

  val tarantoolConnectionLayer: ZLayer[Any, Throwable, TarantoolConnection] =
    (Clock.live ++ configLayer ++ loggingLayer) >>> TarantoolConnection.live

  val socketChannelQueuedWriterLayer: ZLayer[Any, Throwable, SocketChannelQueuedWriter] =
    (configLayer ++ tarantoolConnectionLayer ++ Clock.live ++ loggingLayer) >>> SocketChannelQueuedWriter.live

  val requestHandlerLayer: ZLayer[Any, TarantoolError, RequestHandler] =
    loggingLayer >>> RequestHandler.live

  val schemaMetaManagerLayer: ZLayer[Any, Throwable, SchemaMetaManager] =
    (configLayer ++ requestHandlerLayer ++ socketChannelQueuedWriterLayer ++ syncIdProviderLayer ++ Clock.live ++ loggingLayer) >>> SchemaMetaManager.live

  val delayedQueueLayer: ZLayer[Any, Throwable, DelayedQueue] =
    (loggingLayer ++ schemaMetaManagerLayer ++ requestHandlerLayer) >>> DelayedQueue.live

  val responseHandlerLayer: ZLayer[Any, Throwable, ResponseHandler] =
    (tarantoolConnectionLayer ++ requestHandlerLayer ++ delayedQueueLayer ++ loggingLayer) >>> ResponseHandler.live

  val communicationFacadeLayer: ZLayer[Any, Throwable, CommunicationFacade] =
    (loggingLayer ++ schemaMetaManagerLayer ++ requestHandlerLayer ++ socketChannelQueuedWriterLayer ++ syncIdProviderLayer ++ responseHandlerLayer) >>> CommunicationFacade.live

  val tarantoolClientLayer: ZLayer[Any, Throwable, TarantoolClient] =
    (loggingLayer ++ Clock.live ++ configLayer) >>> TarantoolClient.live

  val tarantoolClientNotMetaCacheLayer: ZLayer[Any, Throwable, TarantoolClient] =
    (loggingLayer ++ Clock.live ++ configNoMetaCacheLayer) >>> TarantoolClient.live
}
