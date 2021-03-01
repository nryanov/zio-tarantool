package zio.tarantool.core

import zio._
import zio.logging._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.{TarantoolConfig, TarantoolError}
import zio.tarantool.protocol.Constants._
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.Selector

@accessible[TarantoolConnection.Service]
object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def connect(): ZIO[Any, Throwable, Unit]

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
    make0(config)

  private def make0(config: TarantoolConfig): ZManaged[Logging with Clock, Throwable, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      channelProvider <- SocketChannelProvider.make(config)
      connected <- Ref.make(false).toManaged_
    } yield new Live(logger, config, channelProvider, connected)

  private[this] final class Live(
    logger: Logger[String],
    tarantoolConfig: TarantoolConfig,
    channel: SocketChannelProvider.Service,
    connected: Ref[Boolean]
  ) extends TarantoolConnection.Service {

    override def connect(): ZIO[Any, Throwable, Unit] =
      ZIO.ifM(isConnected)(
        ZIO.unit,
        greeting().zipRight(channel.blockingMode(false)).orDie.unit *> connected.set(true)
      )

    override def isConnected: UIO[Boolean] = connected.get

    override def sendRequest(buffer: ByteBuffer): IO[TarantoolError, Option[Int]] =
      channel.writeFully(buffer)

    override def isBlocking: UIO[Boolean] = channel.isBlocking()

    override def registerSelector(
      selector: Selector,
      selectionKey: Int
    ): IO[TarantoolError.IOError, Unit] = channel.registerSelector(selector, selectionKey)

    override def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] = channel.read(buffer)

    private def greeting(): ZIO[Any, Throwable, String] = for {
      buffer <- ZIO(ByteBuffer.allocate(GreetingLength))
      _ <- channel.read(buffer)
      firstLine = new String(buffer.array())
      _ <- logger.info(firstLine)
      _ <- ZIO.effectTotal(buffer.clear())
      _ <- channel.read(buffer)
      salt = new String(buffer.array())
    } yield salt

    // todo: auth
    private def auth(): IO[IOException, Unit] = ???
  }
}
