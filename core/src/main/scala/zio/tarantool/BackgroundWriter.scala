package zio.tarantool

import java.nio.ByteBuffer

import zio.tarantool.SocketChannelProvider.SocketChannelProvider
import zio.tarantool.impl.BackgroundWriterLive
import zio.{Has, RIO, Semaphore, ZIO, ZLayer}

import scala.concurrent.ExecutionContext

object BackgroundWriter {
  type BackgroundWriter = Has[Service]

  trait Service extends Serializable {
    def write(buffer: ByteBuffer): ZIO[Any, Throwable, Int]
  }

  def live(ec: ExecutionContext): ZLayer[SocketChannelProvider, Nothing, BackgroundWriter] = (for {
    sem <- Semaphore.make(1)
    channelProvider <- ZIO.access[SocketChannelProvider](_.get)
  } yield new BackgroundWriterLive(channelProvider, ec, sem)).toLayer

  def write(buffer: ByteBuffer): RIO[BackgroundWriter, Int] =
    ZIO.accessM(_.get.write(buffer))
}
