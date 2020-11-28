package zio.tarantool

import zio.tarantool.protocol.{AuthInfo, OperationCode}
import zio.tarantool.msgpack._
import zio._
import zio.tarantool.BackgroundReader.BackgroundReader
import zio.tarantool.PacketManager.PacketManager
import zio.tarantool.SocketChannelProvider.SocketChannelProvider
import zio.tarantool.impl.TarantoolConnectionLive

object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def connect(): ZIO[Any, Throwable, Unit]

    def connect(authInfo: AuthInfo): ZIO[Any, Throwable, Unit]

    def send(op: OperationCode, body: Map[Long, MessagePack]): ZIO[Any, Throwable, TarantoolOperation]

    def close(): ZIO[Any, Throwable, Unit]
  }

  def live: ZLayer[SocketChannelProvider with PacketManager with BackgroundReader, Throwable, TarantoolConnection] =
    ZManaged
      .make(
        for {
          backgroundReader <- ZIO.service[BackgroundReader.Service]
          channelProvider <- ZIO.service[SocketChannelProvider.Service]
          manager <- ZIO.service[PacketManager.Service]
          connection = new TarantoolConnectionLive(channelProvider, manager, backgroundReader)
          _ <- connection.connect()
        } yield connection
      )(connection => connection.close().orDie)
      .toLayer

  def connect(): ZIO[TarantoolConnection, Throwable, Unit] = ZIO.accessM(_.get.connect())

  def connect(authInfo: AuthInfo): ZIO[TarantoolConnection, Throwable, Unit] = ZIO.accessM(_.get.connect(authInfo))

  def send(op: OperationCode, body: Map[Long, MessagePack]): ZIO[TarantoolConnection, Throwable, TarantoolOperation] =
    ZIO.accessM(_.get.send(op, body))

  def close(): ZIO[TarantoolConnection, Throwable, Unit] = ZIO.accessM(_.get.close())
}
