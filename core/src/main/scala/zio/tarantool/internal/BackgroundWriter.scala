package zio.tarantool.internal

import java.nio.ByteBuffer

import SocketChannelProvider.SocketChannelProvider
import zio.tarantool.internal.impl.BackgroundWriterLive
import zio.{Has, RIO, Semaphore, ZIO, ZLayer, ZManaged}

private[tarantool] object BackgroundWriter {
  type BackgroundWriter = Has[Service]

  trait Service extends Serializable {
    def write(buffer: ByteBuffer): ZIO[Any, Throwable, Int]

    def close(): ZIO[Any, Throwable, Unit]
  }

  def live(): ZLayer[SocketChannelProvider, Nothing, BackgroundWriter] =
    ZLayer.fromServiceManaged[SocketChannelProvider.Service, Any, Nothing, Service](scp => make(scp))

  def make(scp: SocketChannelProvider.Service): ZManaged[Any, Nothing, Service] =
    ZManaged.make(
      Semaphore.make(1).map(new BackgroundWriterLive(scp, ExecutionContextManager.singleThreaded(), _))
    )(_.close().orDie)

  def write(buffer: ByteBuffer): RIO[BackgroundWriter, Int] =
    ZIO.accessM(_.get.write(buffer))

  def close(): RIO[BackgroundWriter, Unit] =
    ZIO.accessM(_.get.close())
}
