package zio.tarantool.internal

import java.nio.ByteBuffer

import SocketChannelProvider.SocketChannelProvider
import zio.tarantool.TarantoolError
import zio.tarantool.internal.impl.BackgroundWriterLive
import zio.{Has, IO, Semaphore, ZIO, ZLayer, ZManaged}

private[tarantool] object BackgroundWriter {
  type BackgroundWriter = Has[Service]

  trait Service extends Serializable {
    def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Int]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  def live(): ZLayer[SocketChannelProvider, Nothing, BackgroundWriter] =
    ZLayer.fromServiceManaged[SocketChannelProvider.Service, Any, Nothing, Service](scp =>
      make(scp)
    )

  def make(scp: SocketChannelProvider.Service): ZManaged[Any, Nothing, Service] =
    ZManaged.make(
      Semaphore
        .make(1)
        .map(new BackgroundWriterLive(scp, ExecutionContextManager.singleThreaded(), _))
    )(_.close().orDie)

  def write(buffer: ByteBuffer): ZIO[BackgroundWriter, TarantoolError.IOError, Int] =
    ZIO.accessM(_.get.write(buffer))

  def close(): ZIO[BackgroundWriter, TarantoolError.IOError, Unit] =
    ZIO.accessM(_.get.close())
}
