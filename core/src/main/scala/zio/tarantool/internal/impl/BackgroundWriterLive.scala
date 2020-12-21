package zio.tarantool.internal.impl

import java.nio.ByteBuffer

import zio.clock.Clock
import zio.duration._
import zio.internal.Executor
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool._
import zio.tarantool.internal.impl.BackgroundWriterLive._
import zio.tarantool.internal.{BackgroundWriter, ExecutionContextManager, SocketChannelProvider}
import zio.{Queue, Semaphore, UIO, ZIO}

import scala.util.control.NoStackTrace

private[tarantool] final class BackgroundWriterLive(
  tarantoolConfig: TarantoolConfig,
  channelProvider: SocketChannelProvider.Service,
  ec: ExecutionContextManager,
  directWriteSemaphore: Semaphore,
  queue: Queue[ByteBuffer],
  clock: Clock
) extends BackgroundWriter.Service
    with Logging {

  private val writeTimeout = tarantoolConfig.clientConfig.writeTimeoutMillis

  override def write(buffer: ByteBuffer): ZIO[Any, TarantoolError.IOError, Unit] =
    directWrite(buffer).refineOrDie(toIOError)

  def start(): UIO[Unit] =
    start0().forever.lock(Executor.fromExecutionContext(1000)(ec.executionContext)).fork.unit

  override def close(): ZIO[Any, TarantoolError.IOError, Unit] =
    (debug("Close BackgroundWriter") *> ec.shutdown()).refineOrDie(toIOError)

  private def start0(): ZIO[Any, Throwable, Unit] = for {
    buffer <- queue.take
    // just wait permission
    _ <- directWriteSemaphore.withPermitManaged.use { _ =>
      writeFully(buffer)
    }
  } yield ()

  private def directWrite(buffer: ByteBuffer): ZIO[Any, Throwable, Unit] = for {
    _ <- directWriteSemaphore.withPermitManaged
      .timeout(writeTimeout.milliseconds)
      .provide(clock)
      .use {
        case Some(_) =>
          writeFully(buffer).flatMap(bytes => debug(s"[direct write] bytes sent: $bytes"))
        case None => placeToQueue(buffer)
      }
  } yield ()

  private def placeToQueue(buffer: ByteBuffer): ZIO[Any, Nothing, Unit] =
    (debug("Send request to queue") *> queue.offer(buffer)).fork.unit

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
