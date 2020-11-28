package zio.tarantool.impl

import java.nio.ByteBuffer
import java.nio.channels.{SelectionKey, Selector}
import java.nio.channels.spi.{AbstractSelector, SelectorProvider}

import scodec.bits.ByteVector
import zio._
import zio.internal.Executor
import zio.tarantool.BackgroundReader.Service
import zio.tarantool.SocketChannelProvider
import zio.tarantool.impl.BackgroundReaderLive.MessagePackPacketReadError
import zio.tarantool.internal.Logging
import zio.tarantool.msgpack.Implicits._
import zio.tarantool.protocol.Constants.MessageSizeLength
import zio.tarantool.protocol.{MessagePackPacket, MessagePackPacketCodec}

import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace

class BackgroundReaderLive(channelProvider: SocketChannelProvider.Service, ec: ExecutionContext) extends Service with Logging {

  override def start(completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]): ZIO[Any, Throwable, Unit] =
    ZIO.ifM(ZIO.succeed(channelProvider.channel.isBlocking))(
      ZIO.fail(new IllegalArgumentException("Channel should be in non-blocking mode")),
      start0(completeHandler)
    )

  private def start0(completeHandler: MessagePackPacket => ZIO[Any, Throwable, Unit]): ZIO[Any, Throwable, Unit] = {
    val selector: Selector = SelectorProvider.provider.openSelector
    channelProvider.channel.register(selector, SelectionKey.OP_READ)
//    read(selector).flatMap(completeHandler).forever.lock(Executor.fromExecutionContext(1000)(ec)).fork.unit
    read(selector).flatMap(completeHandler).forever.forkOn(ec).unit
  }

  private def read(selector: Selector): ZIO[Any, Throwable, MessagePackPacket] = for {
    buffer <- ZIO.effectTotal(ByteBuffer.allocate(MessageSizeLength))
    bytes <- readBuffer(buffer, selector)
    _ <- debug(s"Read bytes: $bytes")
    vector = ByteVector(buffer.flip())
    size <- ZIO.effect(vector.decode().require.toNumber.toInt)
    messageBuffer <- ZIO.effectTotal(ByteBuffer.allocate(size))
    _ <- readBuffer(messageBuffer, selector)
    packet <- ZIO.effect(MessagePackPacketCodec.decodeValue(ByteVector(messageBuffer.flip()).toBitVector).require)
  } yield packet

  private def readBuffer(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] = {
    var total: Int = 0

    for {
      read <- channelProvider.read(buffer).tap(r => ZIO.effectTotal(total += r))
      _ <- if (read < 0) ZIO.fail(MessagePackPacketReadError) else ZIO.unit
      _ <- readViaSelector(buffer, selector).tap(r => ZIO.effectTotal(total += r))
    } yield total
  }

  private def readViaSelector(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] = for {
    _ <- debug("readViaSelector")
    _ <- ZIO.effect(selector.select())
    read <- channelProvider.read(buffer)
    _ <- debug(s"Read bytes: $read")
    total <- if (buffer.remaining() > 0) readViaSelector(buffer, selector).map(_ + read) else ZIO.succeed(read)
  } yield total
}

object BackgroundReaderLive {
  case object MessagePackPacketReadError extends RuntimeException("Error while reading message pack packet") with NoStackTrace
}
