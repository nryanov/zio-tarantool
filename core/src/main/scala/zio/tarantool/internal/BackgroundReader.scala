package zio.tarantool.internal

import zio.tarantool.TarantoolError
import zio.tarantool.internal.impl.BackgroundReaderLive
import zio.tarantool.protocol.MessagePackPacket
import zio.{Has, IO, RIO, ZIO, ZLayer, ZManaged}

import SocketChannelProvider.SocketChannelProvider

private[tarantool] object BackgroundReader {
  type BackgroundReader = Has[Service]

  trait Service extends Serializable {
    def start(
      completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
    ): IO[TarantoolError.IOError, Unit]

    def close(): IO[TarantoolError.IOError, Unit]
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
    completeHandler: MessagePackPacket => ZIO[Any, TarantoolError.IOError, Unit]
  ): RIO[BackgroundReader, Unit] =
    ZIO.accessM(_.get.start(completeHandler))

  def close(): ZIO[BackgroundReader, TarantoolError.IOError, Unit] =
    ZIO.accessM(_.get.close())
}
