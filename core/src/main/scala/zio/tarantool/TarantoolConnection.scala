package zio.tarantool

import zio._
import zio.tarantool.msgpack._
import zio.tarantool.protocol.{AuthInfo, OperationCode}
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

    def connect(authInfo: AuthInfo): ZIO[Any, Throwable, Unit]

    def send(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): ZIO[Any, Throwable, TarantoolOperation]
  }

  def live(): ZLayer[Has[ClientConfig], Throwable, TarantoolConnection] =
    ZLayer.fromServiceManaged[ClientConfig, Any, Throwable, Service](cfg => make(cfg))

  def make(config: ClientConfig): ZManaged[Any, Throwable, Service] =
    make0(config).tapM(_.connect())

  private def make0(config: ClientConfig): ZManaged[Any, Throwable, Service] = for {
    channelProvider <- SocketChannelProvider.make(config)
    reader <- BackgroundReader.make(channelProvider)
    writer <- BackgroundWriter.make(channelProvider)
    packetManager <- PacketManager.make()
  } yield new TarantoolConnectionLive(channelProvider, packetManager, reader, writer)

  def connect(): ZIO[TarantoolConnection, Throwable, Unit] = ZIO.accessM(_.get.connect())

  def connect(authInfo: AuthInfo): ZIO[TarantoolConnection, Throwable, Unit] =
    ZIO.accessM(_.get.connect(authInfo))

  def send(
    op: OperationCode,
    body: Map[Long, MessagePack]
  ): ZIO[TarantoolConnection, Throwable, TarantoolOperation] =
    ZIO.accessM(_.get.send(op, body))
}
