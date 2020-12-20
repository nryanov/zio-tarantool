package zio.tarantool.internal.impl

import java.nio.ByteBuffer

import zio.tarantool.TarantoolError.toIOError
import zio.tarantool._
import zio.tarantool.internal.impl.BackgroundWriterLive._
import zio.tarantool.internal.{BackgroundWriter, ExecutionContextManager, SocketChannelProvider}
import zio.{Semaphore, ZIO}

import scala.util.control.NoStackTrace

private[tarantool] final class BackgroundWriterLive(
  channelProvider: SocketChannelProvider.Service,
  ec: ExecutionContextManager,
  directWriteSemaphore: Semaphore
) extends BackgroundWriter.Service
    with Logging {
  def write(buffer: ByteBuffer): ZIO[Any, TarantoolError.IOError, Int] =
    directWrite(buffer).refineOrDie(toIOError)

  override def close(): ZIO[Any, TarantoolError.IOError, Unit] =
    (debug("Close BackgroundWriter") *> ec.shutdown()).refineOrDie(toIOError)

  // todo: add timeout (from TarantoolClientConfig)
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
