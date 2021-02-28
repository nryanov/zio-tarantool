package zio.tarantool

import zio.{Has, ULayer, ZLayer}
import zio.console.Console
import zio.blocking.Blocking
import zio.clock.Clock
import zio.logging.Logging
import zio.tarantool.core._
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.TarantoolContainer.Tarantool
import zio.tarantool.core.CommunicationInterceptor.CommunicationInterceptor
import zio.tarantool.core.PacketManager.PacketManager
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.ResponseHandler.ResponseHandler
import zio.tarantool.core.SchemaIdProvider.SchemaIdProvider
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

  val loggingLayer: ZLayer[Any, Nothing, Logging] =
    (Clock.live ++ Console.live) >>> Logging.console()

  val syncIdProviderLayer: ZLayer[Any, Nothing, SyncIdProvider] = SyncIdProvider.live

  val schemaIdProviderLayer: ZLayer[Any, Nothing, SchemaIdProvider] = SchemaIdProvider.live

  val packetManagerLayer: ULayer[PacketManager] = PacketManager.live

  val tarantoolConnectionLayer: ZLayer[Any, Throwable, TarantoolConnection] =
    (Clock.live ++ configLayer ++ loggingLayer) >>> TarantoolConnection.live

  val socketChannelQueuedWriterLayer: ZLayer[Any, Throwable, SocketChannelQueuedWriter] =
    (configLayer ++ tarantoolConnectionLayer ++ Clock.live ++ loggingLayer) >>> SocketChannelQueuedWriter.live

  val requestHandlerLayer: ZLayer[Any, TarantoolError, RequestHandler] =
    loggingLayer >>> RequestHandler.live

  val responseHandlerLayer: ZLayer[Any, Throwable, ResponseHandler] =
    (tarantoolConnectionLayer ++ packetManagerLayer ++ requestHandlerLayer ++ loggingLayer) >>> ResponseHandler.live

  val schemaMetaManagerLayer: ZLayer[Any, Throwable, SchemaMetaManager] =
    (configLayer ++ requestHandlerLayer ++ socketChannelQueuedWriterLayer ++ syncIdProviderLayer ++ schemaIdProviderLayer ++ Clock.live ++ loggingLayer) >>> SchemaMetaManager.live

  val communicationInterceptorLayer: ZLayer[Any, Throwable, CommunicationInterceptor] =
    (loggingLayer ++ schemaIdProviderLayer ++ requestHandlerLayer ++ socketChannelQueuedWriterLayer ++ syncIdProviderLayer ++ responseHandlerLayer) >>> CommunicationInterceptor.live

  val tarantoolClientLayer: ZLayer[Any, Throwable, TarantoolClient] =
    (loggingLayer ++ communicationInterceptorLayer) >>> TarantoolClient.live
}
