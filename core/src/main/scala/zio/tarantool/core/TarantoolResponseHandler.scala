package zio.tarantool.core

import zio._
import zio.macros.accessible
import zio.internal.Executor
import zio.tarantool._
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.protocol.{Code, MessagePackPacket, TarantoolResponse}
import zio.tarantool.core.PacketManager.PacketManager
import zio.tarantool.protocol.Constants.MessageSizeLength
import zio.tarantool.protocol.Implicits.{RichByteVector, RichMessagePack}
import java.nio.ByteBuffer
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{SelectionKey, Selector}

import scodec.bits.ByteVector
import SocketChannelProvider.SocketChannelProvider
import zio.logging.Logger

@accessible
private[tarantool] object TarantoolResponseHandler {
  type BackgroundReader = Has[Service]

  trait Service extends Serializable {
    def start(): IO[TarantoolError.IOError, Fiber.Runtime[Throwable, Nothing]]

    def complete(packet: MessagePackPacket): IO[TarantoolError, Unit]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  val live: ZLayer[SocketChannelProvider with PacketManager, Nothing, BackgroundReader] = ???
//    ZLayer.fromServicesManaged[
//      SocketChannelProvider.Service,
//      PacketManager.Service,
//      Any,
//      Nothing,
//      Service
//    ]((scp, packetManager) => make(scp, packetManager))
//
//  def make(
//    scp: SocketChannelProvider.Service,
//    packetManager: PacketManager.Service
//  ): ZManaged[Any, Nothing, Service] =
//    ZManaged.make(
//      ZIO.succeed(
//        new Live(
//          ???,
//          scp,
//          packetManager,
//          ???,
//          ???,
//          ???,
//          ExecutionContextManager.singleThreaded()
//        )
//      )
//    )(_.close().orDie)

  private[this] final class Live(
    logger: Logger[String],
    channelProvider: SocketChannelProvider.Service,
    packetManager: PacketManager.Service,
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: TarantoolRequestHandler.Service,
    connection: TarantoolConnection.Service,
    ec: ExecutionContextManager
  ) extends Service {

    override def start(): IO[TarantoolError.IOError, Fiber.Runtime[Throwable, Nothing]] =
      ZIO
        .ifM(channelProvider.isBlocking())(
          ZIO.fail(new IllegalArgumentException("Channel should be in non-blocking mode")),
          start0()
        )
        .refineOrDie(toIOError)

    override def close(): IO[TarantoolError.IOError, Unit] =
      (logger.debug("Close BackgroundReader") *> ec.shutdown()).refineOrDie(toIOError)

    private def start0(): ZIO[Any, TarantoolError.IOError, Fiber.Runtime[Throwable, Nothing]] = {
      val selector: Selector = SelectorProvider.provider.openSelector

      channelProvider.registerSelector(selector, SelectionKey.OP_READ) *>
        read(selector).forever.lock(Executor.fromExecutionContext(1000)(ec.executionContext)).fork
    }

    private def read(
      selector: Selector
    ): ZIO[Any, Throwable, Unit] = for {
      buffer <- ZIO.effectTotal(ByteBuffer.allocate(MessageSizeLength))
      _ <- readBuffer(buffer, selector)
      vector = makeByteVector(buffer)
      size <- vector.decodeM().map(_.toNumber.toInt)
      messageBuffer: ByteBuffer <- ZIO.effectTotal(ByteBuffer.allocate(size))
      _ <- readBuffer(messageBuffer, selector)
      packet <- packetManager.decodeToMessagePackPacket(makeByteVector(messageBuffer))
      _ <- complete(packet)
    } yield ()

    private def readBuffer(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] = {
      var total: Int = 0

      for {
        read <- channelProvider.read(buffer).tap(r => ZIO.effectTotal(total += r))
        _ <- ZIO
          .fail(
            TarantoolError.MessagePackPacketReadError("Error while reading message pack packet")
          )
          .when(read < 0)
        _ <- readViaSelector(buffer, selector)
          .tap(r => ZIO.effectTotal(total += r))
          .when(buffer.remaining() > 0)
      } yield total
    }

    private def readViaSelector(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] =
      for {
        _ <- ZIO.effect(selector.select())
        read <- channelProvider.read(buffer)
        total <-
          if (buffer.remaining() > 0) readViaSelector(buffer, selector).map(_ + read)
          else ZIO.succeed(read)
      } yield total

    private def makeByteVector(buffer: ByteBuffer): ByteVector = {
      buffer.flip()
      ByteVector.view(buffer)
    }

    override def complete(packet: MessagePackPacket): IO[TarantoolError, Unit] =
      for {
        syncId <- MessagePackPacket.extractSyncId(packet)
        schemaId <- MessagePackPacket.extractSchemaId(packet)
        code <- MessagePackPacket.extractCode(packet)
        _ <- logger.debug(s"Complete operation with id: $syncId")
        // todo: check code Success, IncorrectSchema, Error, Sql
        _ <- ZIO.ifM(ZIO.succeed(code == Code.Success.value))(
          MessagePackPacket
            .extractData(packet)
            .flatMap(data => requestHandler.complete(syncId, TarantoolResponse(schemaId, data))),
          MessagePackPacket
            .extractError(packet)
            .flatMap(error => requestHandler.fail(syncId, error))
        )
      } yield ()
  }

}