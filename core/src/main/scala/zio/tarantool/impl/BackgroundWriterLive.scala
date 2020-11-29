package zio.tarantool.impl

import java.nio.ByteBuffer

import zio.{Semaphore, ZIO}
import zio.tarantool.{BackgroundWriter, SocketChannelProvider}
import zio.tarantool.internal.Logging

import scala.concurrent.ExecutionContext
import scala.util.control.NoStackTrace
import BackgroundWriterLive._

final class BackgroundWriterLive(channelProvider: SocketChannelProvider.Service, ec: ExecutionContext, directWriteSemaphore: Semaphore)
    extends BackgroundWriter.Service
    with Logging {
  def write(buffer: ByteBuffer): ZIO[Any, Throwable, Int] = directWrite(buffer)

  private def directWrite(buffer: ByteBuffer): ZIO[Any, Throwable, Int] = for {
    dataSent <- directWriteSemaphore.withPermit(writeFully(buffer))
    _ <- debug(s"[direct write] bytes sent: $dataSent")
  } yield dataSent

  private def writeFully(buffer: ByteBuffer): ZIO[Any, Throwable, Int] = for {
    res <- if (buffer.remaining() > 0) channelProvider.write(buffer) else ZIO.succeed(0)
    _ <- if (res < 0) ZIO.fail(DirectWriteError(buffer)) else ZIO.unit
    total <- if (buffer.remaining() > 0) writeFully(buffer).map(_ + res) else ZIO.succeed(res)
  } yield total
}

object BackgroundWriterLive {
  final case class DirectWriteError(buffer: ByteBuffer)
      extends RuntimeException(s"Error happened while sending buffer: $buffer")
      with NoStackTrace
}
