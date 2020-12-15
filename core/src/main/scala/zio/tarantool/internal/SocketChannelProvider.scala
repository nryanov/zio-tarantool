package zio.tarantool.internal

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel

import zio.clock.Clock
import zio.duration._
import zio.tarantool.{Logging, TarantoolConfig}
import zio.{Has, RIO, Schedule, ZIO, ZLayer, ZManaged}

private[tarantool] object SocketChannelProvider {
  type SocketChannelProvider = Has[Service]

  trait Service extends Serializable {
    def channel: SocketChannel

    def close(): ZIO[Any, Throwable, Unit]

    def read(buffer: ByteBuffer): ZIO[Any, Throwable, Int]

    def write(buffer: ByteBuffer): ZIO[Any, Throwable, Int]

    def blockingMode(flag: Boolean): ZIO[Any, Throwable, Unit]
  }

  final case class Live(channel: SocketChannel) extends Service with Logging {
    override def close(): ZIO[Any, Throwable, Unit] =
      debug("Close socket channel") *> ZIO.effect(channel.close())

    override def read(buffer: ByteBuffer): ZIO[Any, Throwable, Int] =
      ZIO.effect(channel.read(buffer))

    override def write(buffer: ByteBuffer): ZIO[Any, Throwable, Int] =
      ZIO.effect(channel.write(buffer))

    // intentionally blocking
    override def blockingMode(flag: Boolean): ZIO[Any, Throwable, Unit] =
      ZIO.effect(channel.configureBlocking(flag))
  }

  def live(): ZLayer[Has[TarantoolConfig] with Clock, Throwable, SocketChannelProvider] =
    ZLayer.fromServiceManaged[TarantoolConfig, Any with Clock, Throwable, Service](cfg => make(cfg))

  def make(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] =
    ZManaged.make(for {
      channel <- ZIO.effect(SocketChannel.open()).tap { channel =>
        ZIO
          .effect(new InetSocketAddress(config.connectionConfig.host, config.connectionConfig.port))
          .flatMap(address =>
            ZIO
              .effect(channel.connect(address))
              .timeout(config.connectionConfig.connectionTimeoutMillis.milliseconds)
          )
          .retry(
            Schedule
              .recurs(config.connectionConfig.retries)
              .delayed(_ => config.connectionConfig.retryTimeoutMillis.milliseconds)
          )
      }
    } yield Live(channel))(channel => channel.close().orDie)

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
