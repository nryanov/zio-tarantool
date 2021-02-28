package zio.tarantool.core

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{Selector, SocketChannel}

import zio.duration._
import zio.macros.accessible
import zio.clock.Clock
import zio.logging.{Logger, Logging}
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.{TarantoolConfig, TarantoolError}
import zio.{Has, IO, Schedule, Semaphore, UIO, ZIO, ZLayer, ZManaged}

@accessible
private[tarantool] object SocketChannelProvider {
  type SocketChannelProvider = Has[Service]

  trait Service extends Serializable {
    def isBlocking(): UIO[Boolean]

    def registerSelector(selector: Selector, selectionKey: Int): IO[TarantoolError.IOError, Unit]

    def close(): IO[TarantoolError.IOError, Unit]

    def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int]

    def writeFully(buffer: ByteBuffer): IO[TarantoolError, Option[Int]]

    def blockingMode(flag: Boolean): IO[TarantoolError.IOError, Unit]
  }

  final case class Live(
    logger: Logger[String],
    tarantoolConfig: TarantoolConfig,
    channel: SocketChannel,
    writeSemaphore: Semaphore,
    clock: Clock
  ) extends Service {
    private val writeTimeout = tarantoolConfig.clientConfig.writeTimeoutMillis

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

    override def writeFully(buffer: ByteBuffer): ZIO[Any, TarantoolError, Option[Int]] =
      writeSemaphore.withPermitManaged.timeout(writeTimeout.milliseconds).provide(clock).use {
        case Some(_) =>
          writeFully0(buffer).map(Some(_))
        case None => ZIO.none
      }

    override def blockingMode(flag: Boolean): IO[TarantoolError.IOError, Unit] =
      IO.effect(channel.configureBlocking(flag)).unit.refineOrDie(toIOError)

    private def writeFully0(buffer: ByteBuffer): IO[TarantoolError, Int] = for {
      res <- if (buffer.remaining() > 0) write(buffer) else ZIO.succeed(0)
      _ <- ZIO
        .fail(TarantoolError.DirectWriteError(s"Error happened while sending buffer: $buffer"))
        .when(res < 0)
      total <- if (buffer.remaining() > 0) writeFully0(buffer).map(_ + res) else ZIO.succeed(res)
    } yield total

    private def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] =
      IO.effect(channel.write(buffer)).refineOrDie(toIOError)
  }

  val live: ZLayer[Has[TarantoolConfig] with Clock with Logging, Throwable, SocketChannelProvider] =
    ZLayer.fromServiceManaged[TarantoolConfig, Logging with Clock, Throwable, Service](cfg =>
      make(cfg)
    )

  def make(config: TarantoolConfig): ZManaged[Logging with Clock, Throwable, Service] =
    ZManaged.make(for {
      logger <- ZIO.service[Logger[String]]
      clock <- ZIO.environment[Clock]
      semaphore <- Semaphore.make(1)
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
    } yield Live(logger, config, channel, semaphore, clock))(channel => channel.close().orDie)
}
