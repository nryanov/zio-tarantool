package zio.tarantool

import java.io.IOException
import java.net.InetSocketAddress
import java.nio.channels.AsynchronousSocketChannel
import java.time.Duration

import zio.tarantool.protocol.{AuthInfo, OperationCode}
import zio.tarantool.msgpack._
import zio._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.interop.javaz.fromFutureJava
import zio.tarantool.PacketManager.PacketManager
import zio.tarantool.impl.TarantoolConnectionLive

object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service {
    def connect(): ZIO[Any, IOException, Unit]

    def connect(authInfo: AuthInfo): ZIO[Any, IOException, Unit]

    def read(): ZIO[Any, Throwable, MessagePack]

    def write(op: OperationCode, body: MpMap): ZIO[Any, Throwable, Int]
  }

  def live: ZLayer[Blocking with Clock with Has[ClientConfig] with PacketManager, Throwable, TarantoolConnection] =
    ZLayer.fromEffect {
      for {
        config <- ZIO.service[ClientConfig]
        manager <- ZIO.service[PacketManager.Service]
        connection <- ZIO
          .effect(AsynchronousSocketChannel.open())
          .tap { channel =>
            ZIO
              .effect(new InetSocketAddress(config.host, config.port))
              .flatMap(address => fromFutureJava(channel.connect(address)).timeout(Duration.ofSeconds(5))) // todo: move to config
          }
          .map(channel => new TarantoolConnectionLive(channel, manager))
          .tap(connection => connection.connect())
      } yield connection
    }

  def connect(): ZIO[TarantoolConnection, IOException, Unit] = ZIO.accessM(_.get.connect())

  def connect(authInfo: AuthInfo): ZIO[TarantoolConnection, IOException, Unit] = ZIO.accessM(_.get.connect(authInfo))

  def read(): ZIO[TarantoolConnection, Throwable, MessagePack] = ZIO.accessM(_.get.read())

  def write(op: OperationCode, body: MpMap): ZIO[TarantoolConnection, Throwable, Int] = ZIO.accessM(_.get.write(op, body))
}
