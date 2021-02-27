package zio.tarantool.core

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{Selector, SocketChannel}

import zio.duration._
import zio.macros.accessible
import zio.clock.Clock
import zio.logging.Logger
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.{Logging, TarantoolConfig, TarantoolError}
import zio.{Has, IO, Schedule, UIO, ZIO, ZLayer, ZManaged}

@accessible
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

  final case class Live(logger: Logger[String], channel: SocketChannel) extends Service {
    override def isBlocking(): UIO[Boolean] = UIO.effectTotal(channel.isBlocking)

    override def registerSelector(
      selector: Selector,
      selectionKey: Int
    ): IO[TarantoolError.IOError, Unit] =
      IO.effect(channel.register(selector, selectionKey)).unit.refineOrDie(toIOError)

    override def close(): IO[TarantoolError.IOError, Unit] =
      (logger.debug("Close socket channel") *> ZIO.effect(channel.close())).refineOrDie(toIOError)

    override def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] =
      IO.effect(channel.read(buffer)).refineOrDie(toIOError)

    override def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] =
      IO.effect(channel.write(buffer)).refineOrDie(toIOError)

    override def blockingMode(flag: Boolean): IO[TarantoolError.IOError, Unit] =
      IO.effect(channel.configureBlocking(flag)).unit.refineOrDie(toIOError)
  }

  val live: ZLayer[Has[TarantoolConfig] with Clock, Throwable, SocketChannelProvider] = ???
//    ZLayer.fromServiceManaged[TarantoolConfig, Any with Clock, Throwable, Service](cfg => make(cfg))

//  def make(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] =
//    ZManaged.make(for {
//      channel <- ZIO.effect(SocketChannel.open()).tap { channel =>
//        ZIO
//          .effect(new InetSocketAddress(config.connectionConfig.host, config.connectionConfig.port))
//          .flatMap(address =>
//            ZIO
//              .effect(channel.connect(address))
//              .timeout(config.connectionConfig.connectionTimeoutMillis.milliseconds)
//          )
//          .retry(
//            Schedule
//              .recurs(config.connectionConfig.retries)
//              .delayed(_ => config.connectionConfig.retryTimeoutMillis.milliseconds)
//          )
//      }
//    } yield Live(channel))(channel => channel.close().orDie)
}
