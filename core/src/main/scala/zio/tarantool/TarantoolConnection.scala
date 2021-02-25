package zio.tarantool

import zio._
import zio.macros.accessible
import zio.clock.Clock
import zio.tarantool.msgpack._
import zio.tarantool.protocol.{OperationCode, TarantoolOperation}
import zio.tarantool.impl.TarantoolConnectionLive
import zio.tarantool.internal.{
  BackgroundReader,
  BackgroundWriter,
  PacketManager,
  SocketChannelProvider
}

@accessible
object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def connect(): ZIO[Any, Throwable, Unit]

    def send(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation]
  }

  val live: ZLayer[Has[TarantoolConfig] with Clock, Throwable, TarantoolConnection] =
    ZLayer.fromServiceManaged[TarantoolConfig, Any with Clock, Throwable, Service](cfg => make(cfg))

  def make(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] =
    make0(config).tapM(_.connect())

  private def make0(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] = for {
    channelProvider <- SocketChannelProvider.make(config)
    packetManager <- PacketManager.make()
    reader <- BackgroundReader.make(channelProvider, packetManager)
    writer <- BackgroundWriter.make(config, channelProvider)
  } yield new TarantoolConnectionLive(config, channelProvider, packetManager, reader, writer)
}
