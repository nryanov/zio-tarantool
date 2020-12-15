package zio.tarantool

import zio._
import zio.clock.Clock
import zio.tarantool.msgpack._
import zio.tarantool.protocol.OperationCode
import zio.tarantool.impl.TarantoolConnectionLive
import zio.tarantool.internal.{
  BackgroundReader,
  BackgroundWriter,
  PacketManager,
  SocketChannelProvider
}

object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def connect(): ZIO[Any, Throwable, Unit]

    def send(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): ZIO[Any, Throwable, TarantoolOperation]
  }

  def live(): ZLayer[Has[TarantoolConfig] with Clock, Throwable, TarantoolConnection] =
    ZLayer.fromServiceManaged[TarantoolConfig, Any with Clock, Throwable, Service](cfg => make(cfg))

  def make(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] =
    make0(config).tapM(_.connect())

  private def make0(config: TarantoolConfig): ZManaged[Any with Clock, Throwable, Service] = for {
    channelProvider <- SocketChannelProvider.make(config)
    reader <- BackgroundReader.make(channelProvider)
    writer <- BackgroundWriter.make(channelProvider)
    packetManager <- PacketManager.make()
  } yield new TarantoolConnectionLive(config, channelProvider, packetManager, reader, writer)

  def connect(): ZIO[TarantoolConnection, Throwable, Unit] = ZIO.accessM(_.get.connect())

  def send(
    op: OperationCode,
    body: Map[Long, MessagePack]
  ): ZIO[TarantoolConnection, Throwable, TarantoolOperation] =
    ZIO.accessM(_.get.send(op, body))
}
