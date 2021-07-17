package zio.tarantool

import zio.{BootstrapRuntime, ZIO, ZLayer}
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.clock.Clock
import zio.internal.Platform

trait BenchmarkBase extends BootstrapRuntime {

  override val platform: Platform = Platform.benchmark

  def zioUnsafeRun[A](fa: ZIO[TarantoolClient, TarantoolError, A]): A = unsafeRun(
    fa.provideLayer(BenchmarkBase.layer)
  )
}

object BenchmarkBase {
  private final val layer: ZLayer[Any, TarantoolError, TarantoolClient] =
    (Clock.live ++ ZLayer.succeed(
      TarantoolConfig(
        connectionConfig = ConnectionConfig(host = "localhost", port = 3301),
        clientConfig = ClientConfig(useSchemaMetaCache = false),
        authInfo = None
      )
    )) >>> TarantoolClient.live
}
