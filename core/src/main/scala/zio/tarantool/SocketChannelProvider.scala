package zio.tarantool

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import zio.{Has, RIO, ZIO, ZLayer, ZManaged}

object SocketChannelProvider {
  type SocketChannelProvider = Has[Service]

  trait Service extends Serializable {
    def channel: SocketChannel

    def close(): ZIO[Any, Throwable, Unit]

    def read(buffer: ByteBuffer): ZIO[Any, Throwable, Int]

    def write(buffer: ByteBuffer): ZIO[Any, Throwable, Int]

    def blockingMode(flag: Boolean): ZIO[Any, Throwable, Unit]
  }

  final case class Live(channel: SocketChannel) extends Service {
    override def close(): ZIO[Any, Throwable, Unit] = ZIO.effect(channel.close())

    override def read(buffer: ByteBuffer): ZIO[Any, Throwable, Int] = ZIO.effect(channel.read(buffer))

    override def write(buffer: ByteBuffer): ZIO[Any, Throwable, Int] = ZIO.effect(channel.write(buffer))

    // intentionally blocking
    override def blockingMode(flag: Boolean): ZIO[Any, Throwable, Unit] = ZIO.effect(channel.configureBlocking(flag))
  }

  def live: ZLayer[Has[ClientConfig], Throwable, SocketChannelProvider] =
    ZManaged
      .make[Has[ClientConfig], Has[ClientConfig], Throwable, SocketChannelProvider.Service](for {
        config <- ZIO.service[ClientConfig]
        channel <- ZIO.effect(SocketChannel.open()).tap { channel =>
          ZIO.effect(new InetSocketAddress(config.host, config.port)).flatMap(address => ZIO.effect(channel.connect(address)))
        }
      } yield Live(channel))(channel => channel.close().orDie)
      .toLayer

  def channel(): RIO[SocketChannelProvider, SocketChannel] =
    ZIO.access(_.get.channel)

  def close(): RIO[SocketChannelProvider, Unit] =
    ZIO.accessM(_.get.close())

  def read(buffer: ByteBuffer): RIO[SocketChannelProvider, Int] =
    ZIO.accessM(_.get.read(buffer))

  def write(buffer: ByteBuffer): RIO[SocketChannelProvider, Int] =
    ZIO.accessM(_.get.write(buffer))

  def blockingMode(flag: Boolean): RIO[SocketChannelProvider, Unit] =
    ZIO.accessM(_.get.blockingMode(flag))
}
