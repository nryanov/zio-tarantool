package zio.tarantool

import zio.tarantool.SocketChannelProvider.SocketChannelProvider
import zio.tarantool.impl.BackgroundReaderLive
import zio.{Has, RIO, ZIO, ZLayer}
import zio.tarantool.protocol.MessagePackPacket

import scala.concurrent.ExecutionContext

object BackgroundReader {
  type BackgroundReader = Has[Service]

  trait Service extends Serializable {
    def start(completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]): ZIO[Any, Throwable, Unit]
  }

  def live(ec: ExecutionContext): ZLayer[SocketChannelProvider, Nothing, BackgroundReader] =
    ZLayer.fromService(new BackgroundReaderLive(_, ec))

  def start(completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]): RIO[BackgroundReader, Unit] =
    ZIO.accessM(_.get.start(completeHandler))
}
