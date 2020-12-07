package zio.tarantool.internal.impl

import java.nio.ByteBuffer
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{SelectionKey, Selector}

import scodec.bits.ByteVector
import zio._
import zio.tarantool.Logging
import zio.tarantool.internal.BackgroundReader.Service
import zio.tarantool.internal.{ExecutionContextManager, SocketChannelProvider}
import zio.tarantool.internal.impl.BackgroundReaderLive.MessagePackPacketReadError
import zio.tarantool.msgpack.Implicits._
import zio.tarantool.protocol.Constants.MessageSizeLength
import zio.tarantool.protocol.{MessagePackPacket, MessagePackPacketCodec}

import scala.util.control.NoStackTrace

private[tarantool] final class BackgroundReaderLive(channelProvider: SocketChannelProvider.Service, ec: ExecutionContextManager)
    extends Service
    with Logging {

  override def start(completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]): ZIO[Any, Throwable, Unit] =
    ZIO.ifM(ZIO.succeed(channelProvider.channel.isBlocking))(
      ZIO.fail(new IllegalArgumentException("Channel should be in non-blocking mode")),
      start0(completeHandler)
    )

  override def close(): ZIO[Any, Throwable, Unit] = ec.shutdown()

  private def start0(completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]): ZIO[Any, Throwable, Unit] = {
    val selector: Selector = SelectorProvider.provider.openSelector
    channelProvider.channel.register(selector, SelectionKey.OP_READ)
    // todo: use separate thread pool / thread
//    read(selector).flatMap(completeHandler).forever.lock(Executor.fromExecutionContext(1000)(ec)).fork.unit
    read(selector, completeHandler).forever.fork.unit
  }

  private def read(
    selector: Selector,
    completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]
  ): ZIO[Any, Throwable, Unit] = for {
    buffer <- ZIO.effectTotal(ByteBuffer.allocate(MessageSizeLength))
    bytes <- readBuffer(buffer, selector)
    _ <- debug(s"Total bytes read (size): $bytes")
    vector = ByteVector(buffer.flip())
    size <- ZIO.effect(vector.decode().require.toNumber.toInt)
    messageBuffer <- ZIO.effectTotal(ByteBuffer.allocate(size))
    _ <- debug(s"Message size: $size")
    bytes <- readBuffer(messageBuffer, selector)
    _ <- debug(s"Total bytes read (packet): $bytes")
    packet <- ZIO.effect(MessagePackPacketCodec.decodeValue(ByteVector(messageBuffer.flip()).toBitVector).require)
    _ <- debug(packet.toString)
    _ <- completeHandler(packet)
  } yield ()

  private def readBuffer(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] = {
    var total: Int = 0

    for {
      read <- channelProvider.read(buffer).tap(r => ZIO.effectTotal(total += r))
      _ <- if (read < 0) ZIO.fail(MessagePackPacketReadError) else ZIO.unit
      _ <- if (buffer.remaining() > 0) readViaSelector(buffer, selector).tap(r => ZIO.effectTotal(total += r)) else ZIO.unit
    } yield total
  }

  private def readViaSelector(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] = for {
    _ <- debug("Wait selector")
    _ <- ZIO.effect(selector.select())
    read <- channelProvider.read(buffer)
    total <- if (buffer.remaining() > 0) readViaSelector(buffer, selector).map(_ + read) else ZIO.succeed(read)
    _ <- debug(s"Total read bytes: $total")
  } yield total
}

object BackgroundReaderLive {
  case object MessagePackPacketReadError extends RuntimeException("Error while reading message pack packet") with NoStackTrace
}
