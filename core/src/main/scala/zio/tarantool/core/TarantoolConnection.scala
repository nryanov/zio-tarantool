package zio.tarantool.core

import zio._
import zio.logging._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.{TarantoolConfig, TarantoolError}
import java.io.IOException
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{Selector, SocketChannel}

import zio.tarantool.TarantoolError.toIOError
import zio.duration._

@accessible[TarantoolConnection.Service]
object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def connect(): ZIO[Any, Throwable, Unit]

    def close(): IO[Throwable, Unit]

    def isConnected: UIO[Boolean]

    def sendRequest(buffer: ByteBuffer): IO[TarantoolError, Option[Int]]

    def isBlocking: UIO[Boolean]

    def registerSelector(selector: Selector, selectionKey: Int): IO[TarantoolError.IOError, Unit]

    def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int]
  }

  val live: ZLayer[Logging with Clock with Has[TarantoolConfig], Throwable, Has[Service]] =
    ZLayer.fromServiceManaged[TarantoolConfig, Logging with Clock, Throwable, Service](cfg =>
      make(cfg)
    )

  val test: ZLayer[Any, Nothing, Has[Service]] =
    ZLayer.succeed(new Service {
      override def connect(): ZIO[Any, Throwable, Unit] = IO.unit

      override def close(): IO[Throwable, Unit] = IO.unit

      override def isConnected: UIO[Boolean] = IO.succeed(false)

      override def sendRequest(buffer: ByteBuffer): IO[TarantoolError, Option[Int]] = IO.none

      override def isBlocking: UIO[Boolean] = IO.succeed(false)

      override def registerSelector(
        selector: Selector,
        selectionKey: Int
      ): IO[TarantoolError.IOError, Unit] = IO.unit

      override def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] = IO.succeed(0)
    })

  def make(config: TarantoolConfig): ZManaged[Logging with Clock, Throwable, Service] =
    ZManaged.make(
      for {
        logger <- ZIO.service[Logger[String]]
        channelProvider <- makeSocketChannelProvider(config)
        connected <- Ref.make(false)
        live = new Live(logger, config, channelProvider, connected)
        _ <- live.connect()
      } yield live
    )(_.close().orDie)

  /*
    When a client connects to the server instance,
    the instance responds with a 128-byte text greeting message, not in MsgPack format:
    64-byte Greeting text line 1
    64-byte Greeting text line 2
    44-byte base64-encoded salt
    20-byte NULL
   */
  private val GreetingLength = 64

  private[tarantool] class Live(
    logger: Logger[String],
    tarantoolConfig: TarantoolConfig,
    channel: SocketChannelProvider,
    connected: Ref[Boolean]
  ) extends TarantoolConnection.Service {

    override def connect(): ZIO[Any, Throwable, Unit] =
      ZIO.ifM(isConnected)(
        logger.debug("Already connected").unit,
        logger
          .debug("Trying to crete new connection")
          .zipRight(channel.connect())
          .zipRight(connected.set(true))
          .zipRight(logger.debug("Successfully connected"))
      )

    override def close(): IO[Throwable, Unit] =
      ZIO.ifM(isConnected)(
        channel
          .close()
          .zipRight(connected.set(false))
          .zipRight(logger.debug("Successfully closed connection")),
        logger.debug("Connection is already closed")
      )

    override def isConnected: UIO[Boolean] = connected.get

    override def sendRequest(buffer: ByteBuffer): IO[TarantoolError, Option[Int]] =
      channel.writeFully(buffer)

    override def isBlocking: UIO[Boolean] = channel.isBlocking

    override def registerSelector(
      selector: Selector,
      selectionKey: Int
    ): IO[TarantoolError.IOError, Unit] = channel.registerSelector(selector, selectionKey)

    override def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] = channel.read(buffer)
  }

  private case class SocketChannelProvider(
    logger: Logger[String],
    cfg: TarantoolConfig,
    channel: SocketChannel,
    writeSemaphore: Semaphore,
    clock: Clock
  ) {
    private val writeTimeout = cfg.clientConfig.writeTimeoutMillis

    def connect(): IO[Throwable, Unit] = ZIO
      .effect(new InetSocketAddress(cfg.connectionConfig.host, cfg.connectionConfig.port))
      .flatMap(address =>
        ZIO
          .effect(channel.connect(address))
          .timeout(cfg.connectionConfig.connectionTimeoutMillis.milliseconds)
          .orElseFail(
            new RuntimeException(
              s"Connection timeout to ${cfg.connectionConfig.host}:${cfg.connectionConfig.port}"
            )
          )
          .zipRight(handshake())
      )
      .retry(
        Schedule
          .recurs(cfg.connectionConfig.retries)
          .delayed(_ => cfg.connectionConfig.retryTimeoutMillis.milliseconds)
      )
      .provide(clock)

    private def handshake(): ZIO[Any, Throwable, Unit] =
      greeting().zipRight(blockingMode(false).orDie)

    private def greeting(): ZIO[Any, Throwable, String] = for {
      buffer <- ZIO(ByteBuffer.allocate(GreetingLength))
      _ <- read(buffer)
      firstLine = new String(buffer.array())
      _ <- logger.info(firstLine)
      _ <- ZIO.effectTotal(buffer.clear())
      _ <- read(buffer)
      salt = new String(buffer.array())
    } yield salt

    // todo: auth
    private def auth(): IO[IOException, Unit] = ???

    def isBlocking: UIO[Boolean] = UIO.effectTotal(channel.isBlocking)

    def registerSelector(
      selector: Selector,
      selectionKey: Int
    ): IO[TarantoolError.IOError, Unit] =
      IO.effect(channel.register(selector, selectionKey)).unit.refineOrDie(toIOError)

    def close(): IO[TarantoolError.IOError, Unit] =
      (logger.debug("Close socket channel") *> ZIO.effect(channel.close())).refineOrDie(toIOError)

    def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] =
      IO.effect(channel.read(buffer)).refineOrDie(toIOError)

    def writeFully(buffer: ByteBuffer): ZIO[Any, TarantoolError, Option[Int]] =
      writeSemaphore.withPermitManaged.timeout(writeTimeout.milliseconds).provide(clock).use {
        case Some(_) =>
          writeFully0(buffer).map(Some(_))
        case None => ZIO.none
      }

    def blockingMode(flag: Boolean): IO[TarantoolError.IOError, Unit] =
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

  private def makeSocketChannelProvider(
    config: TarantoolConfig
  ): ZIO[Any with Clock with Has[Logger[String]], Throwable, SocketChannelProvider] =
    for {
      logger <- ZIO.service[Logger[String]]
      clock <- ZIO.environment[Clock]
      semaphore <- Semaphore.make(1)
      channel <- ZIO.effect(SocketChannel.open())
    } yield SocketChannelProvider(logger, config, channel, semaphore, clock)
}
