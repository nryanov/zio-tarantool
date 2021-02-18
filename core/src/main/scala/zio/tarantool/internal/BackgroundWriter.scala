package zio.tarantool.internal

import java.nio.ByteBuffer

import SocketChannelProvider.SocketChannelProvider
import zio.clock.Clock
import zio.macros.accessible
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
  ] with SocketChannelProvider with Clock, Nothing, BackgroundWriter] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      SocketChannelProvider.Service,
      Clock,
      Nothing,
      Service
    ] { (cfg, scp) =>
      make(cfg, scp)
    }

  def make(
    cfg: TarantoolConfig,
    scp: SocketChannelProvider.Service
  ): ZManaged[Clock, Nothing, Service] =
    ZManaged.make(
      for {
        clock <- ZIO.environment[Clock]
        semaphore <- Semaphore.make(1)
        // todo: capacity from cfg
        queue <- Queue.bounded[ByteBuffer](1024)
      } yield new BackgroundWriterLive(
        cfg,
        scp,
        ExecutionContextManager.singleThreaded(),
        semaphore,
        queue,
        clock
      )
    )(_.close().orDie)

  def requestQueue(): ZIO[BackgroundWriter, Nothing, Queue[ByteBuffer]] =
    ZIO.access(_.get.requestQueue)
}
