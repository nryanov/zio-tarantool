package zio.tarantool.internal

import java.nio.ByteBuffer

import SocketChannelProvider.SocketChannelProvider
import zio.clock.Clock
import zio.tarantool.{TarantoolConfig, TarantoolError}
import zio.tarantool.internal.impl.BackgroundWriterLive
import zio.{Has, IO, Queue, Semaphore, UIO, ZIO, ZLayer, ZManaged}

private[tarantool] object BackgroundWriter {
  type BackgroundWriter = Has[Service]

  trait Service extends Serializable {
    def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Unit]

    def start(): UIO[Unit]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  def live(): ZLayer[Has[
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

  def write(buffer: ByteBuffer): ZIO[BackgroundWriter, TarantoolError.IOError, Unit] =
    ZIO.accessM(_.get.write(buffer))

  def start(): ZIO[BackgroundWriter, Nothing, Unit] =
    ZIO.accessM(_.get.start())

  def close(): ZIO[BackgroundWriter, TarantoolError.IOError, Unit] =
    ZIO.accessM(_.get.close())
}
