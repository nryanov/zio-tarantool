package zio.tarantool

import _root_.zio.{Unsafe, ZIO, ZLayer}
import _root_.zio.Clock

trait BenchmarkBase {
  def zioUnsafeRun[A](fa: ZIO[TarantoolClient.Service, TarantoolError, A]): A =
    Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe.run(fa.provideLayer(BenchmarkBase.layer)).getOrThrowFiberFailure()
    }
}

object BenchmarkBase {
  private final val layer: ZLayer[Any, TarantoolError, TarantoolClient.Service] =
    (Clock.live ++ ZLayer.succeed(
      TarantoolConfig(
        connectionConfig = ConnectionConfig(host = "localhost", port = 3301),
        clientConfig = ClientConfig(useSchemaMetaCache = false),
        authInfo = None
      )
    )) >>> TarantoolClient.live
}
