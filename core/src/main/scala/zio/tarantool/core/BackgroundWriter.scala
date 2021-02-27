package zio.tarantool.core

import zio._
import zio.duration._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool._
import zio.internal.Executor
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.TarantoolError.ConfigurationError
import zio.tarantool.{TarantoolConfig, TarantoolError}
import SocketChannelProvider.SocketChannelProvider
import java.nio.ByteBuffer

import zio.logging.Logger

@accessible
private[tarantool] object BackgroundWriter {
  type BackgroundWriter = Has[Service]

  trait Service extends Serializable {
    private[tarantool] def requestQueue: Queue[ByteBuffer]

    def write(buffer: ByteBuffer): IO[TarantoolError.IOError, Unit]

    def start(): UIO[Fiber.Runtime[Throwable, Nothing]]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  val live: ZLayer[Has[
    TarantoolConfig
  ] with SocketChannelProvider with Clock, TarantoolError, BackgroundWriter] = ???
//    ZLayer.fromServicesManaged[
//      TarantoolConfig,
//      SocketChannelProvider.Service,
//      Clock,
//      TarantoolError,
//      Service
//    ] { (cfg, scp) =>
//      make(cfg, scp)
//    }

//  def make(
//    cfg: TarantoolConfig,
//    scp: SocketChannelProvider.Service
//  ): ZManaged[Clock, TarantoolError, Service] =
//    ZManaged.make(
//      for {
//        clock <- ZIO.environment[Clock]
//        semaphore <- Semaphore.make(1)
//        queue <- createQueue(cfg.clientConfig.backgroundQueueSize)
//      } yield new Live(
//        cfg,
//        scp,
//        ExecutionContextManager.singleThreaded(),
//        semaphore,
//        queue,
//        clock
//      )
//    )(_.close().orDie)

  private def createQueue(size: Int): IO[TarantoolError, Queue[ByteBuffer]] = size match {
    case x if x > 0  => Queue.bounded(x)
    case x if x == 0 => Queue.unbounded
    case x =>
      ZIO.fail(ConfigurationError(s"Configuration 'backgroundQueueSize' has incorrect value: $x"))
  }

  def requestQueue(): ZIO[BackgroundWriter, Nothing, Queue[ByteBuffer]] =
    ZIO.access(_.get.requestQueue)

  private[this] final class Live(
    logger: Logger[String],
    tarantoolConfig: TarantoolConfig,
    channelProvider: SocketChannelProvider.Service,
    ec: ExecutionContextManager,
    directWriteSemaphore: Semaphore,
    queue: Queue[ByteBuffer],
    clock: Clock
  ) extends BackgroundWriter.Service {

    private val writeTimeout = tarantoolConfig.clientConfig.writeTimeoutMillis

    override def write(buffer: ByteBuffer): ZIO[Any, TarantoolError.IOError, Unit] =
      directWrite(buffer).refineOrDie(toIOError)

    override def start(): UIO[Fiber.Runtime[Throwable, Nothing]] =
      start0()
        .tapError(err => logger.error(s"Error happened in background worker: $err"))
        .forever
        .lock(Executor.fromExecutionContext(1000)(ec.executionContext))
        .fork

    override def close(): ZIO[Any, TarantoolError.IOError, Unit] =
      for {
        _ <- logger.debug("Shutdown background queue")
        _ <- queue.shutdown
        _ <- logger.debug("Close BackgroundWriter")
        _ <- ec.shutdown().refineOrDie(toIOError)
      } yield ()

    private def start0(): ZIO[Any, Throwable, Unit] = for {
      size <- queue.size
      _ <- logger.debug(s"Queue size: $size. Wait new delayed requests")
      buffer <- queue.take
      _ <- logger.debug("Wait write permission in background")
      // just wait permission
      // todo: add timeout ?
      _ <- directWriteSemaphore.withPermitManaged.use_(writeFully(buffer))
      _ <- logger.debug("Successfully write delayed request")
    } yield ()

    private def directWrite(buffer: ByteBuffer): ZIO[Any, Throwable, Unit] = for {
      _ <- directWriteSemaphore.withPermitManaged
        .timeout(writeTimeout.milliseconds)
        .provide(clock)
        .use {
          case Some(_) =>
            writeFully(buffer).flatMap(bytes => logger.debug(s"[direct write] bytes sent: $bytes"))
          case None => placeToQueue(buffer)
        }
    } yield ()

    private def placeToQueue(buffer: ByteBuffer): ZIO[Any, Nothing, Unit] =
      logger.debug("Send request to queue").zipRight(queue.offer(buffer).fork.unit)

    private def writeFully(buffer: ByteBuffer): ZIO[Any, Throwable, Int] = for {
      res <- if (buffer.remaining() > 0) channelProvider.write(buffer) else ZIO.succeed(0)
      _ <- ZIO
        .fail(TarantoolError.DirectWriteError(s"Error happened while sending buffer: $buffer"))
        .when(res < 0)
      total <- if (buffer.remaining() > 0) writeFully(buffer).map(_ + res) else ZIO.succeed(res)
    } yield total

    // for testing purposes
    override private[tarantool] val requestQueue = queue
  }
}
