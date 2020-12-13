package zio.tarantool.internal

import SocketChannelProvider.SocketChannelProvider
import zio.tarantool.internal.impl.BackgroundReaderLive
import zio.tarantool.protocol.MessagePackPacket
import zio.{Has, RIO, ZIO, ZLayer, ZManaged}

private[tarantool] object BackgroundReader {
  type BackgroundReader = Has[Service]

  trait Service extends Serializable {
    def start(
      completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
    ): ZIO[Any, Throwable, Unit]

    def close(): ZIO[Any, Throwable, Unit]
  }

  def live(): ZLayer[SocketChannelProvider, Nothing, BackgroundReader] =
    ZLayer.fromServiceManaged[SocketChannelProvider.Service, Any, Nothing, Service](scp =>
      make(scp)
    )

  def make(scp: SocketChannelProvider.Service): ZManaged[Any, Nothing, Service] =
    ZManaged.make(
      ZIO.succeed(new BackgroundReaderLive(scp, ExecutionContextManager.singleThreaded()))
    )(_.close().orDie)

  def start(
    completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
  ): RIO[BackgroundReader, Unit] =
    ZIO.accessM(_.get.start(completeHandler))

  def close(): RIO[BackgroundReader, Unit] =
    ZIO.accessM(_.get.close())
}
