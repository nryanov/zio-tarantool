package zio.tarantool.core

import zio._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.protocol.Constants._
import zio.tarantool.protocol._
import zio.tarantool.{Logging, TarantoolConfig, TarantoolError}
import java.io.IOException
import java.nio.ByteBuffer

import zio.logging.Logger

@accessible
object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def connect(): ZIO[Any, Throwable, Unit]

    def send(packet: MessagePackPacket): IO[TarantoolError, Unit]
  }

  val live: ZLayer[Has[TarantoolConfig] with Clock, Throwable, TarantoolConnection] = ???
//    ZLayer.fromServiceManaged[TarantoolConfig, Any with Clock, Throwable, Service](cfg => make(cfg))
//
//  def make(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] =
//    make0(config).tapM(_.connect())
//
//  private def make0(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] = for {
//    channelProvider <- SocketChannelProvider.make(config)
//    packetManager <- PacketManager.make()
//    reader <- TarantoolResponseHandler.make(channelProvider, packetManager)
//    writer <- BackgroundWriter.make(config, channelProvider)
//  } yield new Live(config, channelProvider, packetManager, reader, writer)

  private[this] final class Live(
    logger: Logger[String],
    tarantoolConfig: TarantoolConfig,
    channel: SocketChannelProvider.Service,
    packetManager: PacketManager.Service,
    backgroundReader: TarantoolResponseHandler.Service,
    backgroundWriter: BackgroundWriter.Service
  ) extends TarantoolConnection.Service {

    def connect(): ZIO[Any, Throwable, Unit] =
      greeting()
        .zipRight(channel.blockingMode(false))
        .zipRight(backgroundReader.start())
        .zipRight(backgroundWriter.start())
        .orDie
        .unit

    override def send(packet: MessagePackPacket): IO[TarantoolError, Unit] =
      for {
        buffer <- MessagePackPacket.toBuffer(packet)
        _ <- backgroundWriter.write(buffer)
      } yield ()

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
