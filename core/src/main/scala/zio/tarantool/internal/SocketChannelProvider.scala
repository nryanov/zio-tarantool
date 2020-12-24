package zio.tarantool.internal

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{Selector, SocketChannel}

import zio.duration._
import zio.clock.Clock
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.{Logging, TarantoolConfig, TarantoolError}
import zio.{Has, IO, Schedule, UIO, ZIO, ZLayer, ZManaged}

private[tarantool] object SocketChannelProvider {
  type SocketChannelProvider = Has[Service]

  trait Service extends Serializable {
    def isBlocking(): UIO[Boolean]

    def registerSelector(selector: Selector, selectionKey: Int): IO[TarantoolError.IOError, Unit]

    def close(): IO[TarantoolError.IOError, Unit]

    def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int]

    def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Int]

    def blockingMode(flag: Boolean): IO[TarantoolError.IOError, Unit]
  }

  final case class Live(channel: SocketChannel) extends Service with Logging {
    override def isBlocking(): UIO[Boolean] = UIO.effectTotal(channel.isBlocking)

    override def registerSelector(
      selector: Selector,
      selectionKey: Int
    ): IO[TarantoolError.IOError, Unit] =
      IO.effect(channel.register(selector, selectionKey)).unit.refineOrDie(toIOError)

    override def close(): IO[TarantoolError.IOError, Unit] =
      (debug("Close socket channel") *> ZIO.effect(channel.close())).refineOrDie(toIOError)

    override def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] =
      IO.effect(channel.read(buffer)).refineOrDie(toIOError)

    override def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] =
      IO.effect(channel.write(buffer)).refineOrDie(toIOError)

    override def blockingMode(flag: Boolean): IO[TarantoolError.IOError, Unit] =
      IO.effect(channel.configureBlocking(flag)).unit.refineOrDie(toIOError)
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

  def isBlocking(): ZIO[SocketChannelProvider, Nothing, Unit] =
    ZIO.access(_.get.isBlocking())

  def registerSelector(
    selector: Selector,
    selectionKey: Int
  ): ZIO[SocketChannelProvider, TarantoolError.IOError, Unit] =
    ZIO.access(_.get.registerSelector(selector, selectionKey))

  def close(): ZIO[SocketChannelProvider, TarantoolError.IOError, Unit] =
    ZIO.accessM(_.get.close())

  def read(buffer: ByteBuffer): ZIO[SocketChannelProvider, TarantoolError.IOError, Int] =
    ZIO.accessM(_.get.read(buffer))

  def write(buffer: ByteBuffer): ZIO[SocketChannelProvider, TarantoolError.IOError, Int] =
    ZIO.accessM(_.get.write(buffer))

  def blockingMode(flag: Boolean): ZIO[SocketChannelProvider, TarantoolError.IOError, Unit] =
    ZIO.accessM(_.get.blockingMode(flag))
}
