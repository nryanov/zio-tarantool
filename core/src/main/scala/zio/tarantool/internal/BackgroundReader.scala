package zio.tarantool.internal

import SocketChannelProvider.SocketChannelProvider
import zio.macros.accessible
import zio.{Fiber, Has, IO, ZIO, ZLayer, ZManaged}
import zio.tarantool.TarantoolError
import zio.tarantool.internal.impl.BackgroundReaderLive
import zio.tarantool.protocol.MessagePackPacket
import zio.tarantool.internal.PacketManager.PacketManager

@accessible
private[tarantool] object BackgroundReader {
  type BackgroundReader = Has[Service]

  trait Service extends Serializable {
    def start(
      completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
    ): IO[TarantoolError.IOError, Fiber.Runtime[Throwable, Nothing]]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  val live: ZLayer[SocketChannelProvider with PacketManager, Nothing, BackgroundReader] =
    ZLayer.fromServicesManaged[
      SocketChannelProvider.Service,
      PacketManager.Service,
      Any,
      Nothing,
      Service
    ]((scp, packetManager) => make(scp, packetManager))

  def make(
    scp: SocketChannelProvider.Service,
    packetManager: PacketManager.Service
  ): ZManaged[Any, Nothing, Service] =
    ZManaged.make(
      ZIO.succeed(
        new BackgroundReaderLive(scp, packetManager, ExecutionContextManager.singleThreaded())
      )
    )(_.close().orDie)
}
