package zio.tarantool.internal.impl

import java.nio.ByteBuffer

import zio.clock.Clock
import zio.duration._
import zio.internal.Executor
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool._
import zio.tarantool.internal.impl.BackgroundWriterLive._
import zio.tarantool.internal.{BackgroundWriter, ExecutionContextManager, SocketChannelProvider}
import zio.{Fiber, Queue, Semaphore, UIO, ZIO}

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

  override def start(): UIO[Fiber.Runtime[Throwable, Nothing]] =
    start0()
      .tapError(err => error("Error happened in background worker", err))
      .forever
      .lock(Executor.fromExecutionContext(1000)(ec.executionContext))
      .fork

  override def close(): ZIO[Any, TarantoolError.IOError, Unit] =
    for {
      _ <- debug("Shutdown background queue")
      _ <- queue.shutdown
      _ <- debug("Close BackgroundWriter")
      _ <- ec.shutdown().refineOrDie(toIOError)
    } yield ()

  private def start0(): ZIO[Any, Throwable, Unit] = for {
    size <- queue.size
    _ <- debug(s"Queue size: $size")
    _ <- debug("Wait new delayed requests")
    buffer <- queue.take
    _ <- debug("Wait write permission in background")
    // just wait permission
    // todo: add timeout ?
    _ <- directWriteSemaphore.withPermitManaged.use_(writeFully(buffer))
    _ <- debug("Successfully write delayed request")
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
    debug("Send request to queue").zipRight(queue.offer(buffer).fork.unit)

  private def writeFully(buffer: ByteBuffer): ZIO[Any, Throwable, Int] = for {
    res <- if (buffer.remaining() > 0) channelProvider.write(buffer) else ZIO.succeed(0)
    _ <- ZIO.fail(DirectWriteError(buffer)).when(res < 0)
    total <- if (buffer.remaining() > 0) writeFully(buffer).map(_ + res) else ZIO.succeed(res)
  } yield total

  // for testing purposes
  override private[tarantool] val requestQueue = queue
}

object BackgroundWriterLive {
  final case class DirectWriteError(buffer: ByteBuffer)
      extends RuntimeException(s"Error happened while sending buffer: $buffer")
      with NoStackTrace
}
