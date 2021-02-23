package zio.tarantool.internal

import java.nio.ByteBuffer

import SocketChannelProvider.SocketChannelProvider
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.TarantoolError.ConfigurationError
import zio.tarantool.{TarantoolConfig, TarantoolError}
import zio.tarantool.internal.impl.BackgroundWriterLive
import zio.{Fiber, Has, IO, Queue, Semaphore, UIO, ZIO, ZLayer, ZManaged}

@accessible
private[tarantool] object BackgroundWriter {
  type BackgroundWriter = Has[Service]

  trait Service extends Serializable {
    private[tarantool] def requestQueue: Queue[ByteBuffer]

    def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Unit]

    def start(): UIO[Fiber.Runtime[Throwable, Nothing]]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  val live: ZLayer[Has[
    TarantoolConfig
  ] with SocketChannelProvider with Clock, TarantoolError, BackgroundWriter] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      SocketChannelProvider.Service,
      Clock,
      TarantoolError,
      Service
    ] { (cfg, scp) =>
      make(cfg, scp)
    }

  def make(
    cfg: TarantoolConfig,
    scp: SocketChannelProvider.Service
  ): ZManaged[Clock, TarantoolError, Service] =
    ZManaged.make(
      for {
        clock <- ZIO.environment[Clock]
        semaphore <- Semaphore.make(1)
        queue <- createQueue(cfg.clientConfig.backgroundQueueSize)
      } yield new BackgroundWriterLive(
        cfg,
        scp,
        ExecutionContextManager.singleThreaded(),
        semaphore,
        queue,
        clock
      )
    )(_.close().orDie)

  private def createQueue(size: Int): IO[TarantoolError, Queue[ByteBuffer]] = size match {
    case x if x > 0  => Queue.bounded(x)
    case x if x == 0 => Queue.unbounded
    case x =>
      ZIO.fail(ConfigurationError(s"Configuration 'backgroundQueueSize' has incorrect value: $x"))
  }

  def requestQueue(): ZIO[BackgroundWriter, Nothing, Queue[ByteBuffer]] =
    ZIO.access(_.get.requestQueue)
}
