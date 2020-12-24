package zio.tarantool.internal.impl

import java.nio.ByteBuffer
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{SelectionKey, Selector}

import scodec.bits.ByteVector
import zio._
import zio.internal.Executor
import zio.tarantool._
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.internal.BackgroundReader.Service
import zio.tarantool.internal.{ExecutionContextManager, SocketChannelProvider}
import zio.tarantool.internal.impl.BackgroundReaderLive.MessagePackPacketReadError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.Constants.MessageSizeLength
import zio.tarantool.protocol.{MessagePackPacket, MessagePackPacketCodec}

import scala.util.control.NoStackTrace

private[tarantool] final class BackgroundReaderLive(
  channelProvider: SocketChannelProvider.Service,
  ec: ExecutionContextManager
) extends Service
    with Logging {

  override def start(
    completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
  ): IO[TarantoolError.IOError, Unit] =
    ZIO
      .ifM(channelProvider.isBlocking())(
        ZIO.fail(new IllegalArgumentException("Channel should be in non-blocking mode")),
        start0(completeHandler)
      )
      .refineOrDie(toIOError)

  override def close(): IO[TarantoolError.IOError, Unit] =
    (debug("Close BackgroundReader") *> ec.shutdown()).refineOrDie(toIOError)

  private def start0(
    completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
  ): ZIO[Any, Throwable, Unit] = {
    val selector: Selector = SelectorProvider.provider.openSelector

    channelProvider.registerSelector(selector, SelectionKey.OP_READ) *>
      read(selector, completeHandler).forever
        .lock(Executor.fromExecutionContext(1000)(ec.executionContext))
        .fork
        .unit
  }

  private def read(
    selector: Selector,
    completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
  ): ZIO[Any, Throwable, Unit] = for {
    buffer <- ZIO.effectTotal(ByteBuffer.allocate(MessageSizeLength))
    _ <- readBuffer(buffer, selector)
    vector = ByteVector(buffer.flip())
    size <- vector.decodeM().map(_.toNumber.toInt)
    messageBuffer <- ZIO.effectTotal(ByteBuffer.allocate(size))
    _ <- readBuffer(messageBuffer, selector)
    packet <- ZIO.effect(
      MessagePackPacketCodec.decodeValue(ByteVector(messageBuffer.flip()).toBitVector).require
    )
    _ <- completeHandler(packet)
  } yield ()

  private def readBuffer(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] = {
    var total: Int = 0

    for {
      read <- channelProvider.read(buffer).tap(r => ZIO.effectTotal(total += r))
      _ <- if (read < 0) ZIO.fail(MessagePackPacketReadError) else ZIO.unit
      _ <-
        if (buffer.remaining() > 0)
          readViaSelector(buffer, selector).tap(r => ZIO.effectTotal(total += r))
        else ZIO.unit
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
}

object BackgroundReaderLive {
  case object MessagePackPacketReadError
      extends RuntimeException("Error while reading message pack packet")
      with NoStackTrace
}
