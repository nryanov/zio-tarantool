package zio.tarantool.core

import zio._
import zio.logging._
import zio.macros.accessible
import zio.internal.Executor
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.TarantoolError.ConfigurationError
import zio.tarantool.{TarantoolConfig, TarantoolError}
import java.nio.ByteBuffer

import zio.tarantool.core.TarantoolConnection.TarantoolConnection
import zio.tarantool.protocol.MessagePackPacket

@accessible[SocketChannelQueuedWriter.Service]
private[tarantool] object SocketChannelQueuedWriter {
  type SocketChannelQueuedWriter = Has[Service]

  trait Service extends Serializable {
    private[tarantool] def requestQueue: Queue[ByteBuffer]

    def send(packet: MessagePackPacket): IO[TarantoolError, Unit]

    def start(): UIO[Fiber.Runtime[Throwable, Nothing]]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  val live: ZLayer[Has[
    TarantoolConfig
  ] with TarantoolConnection with Logging, TarantoolError, SocketChannelQueuedWriter] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      TarantoolConnection.Service,
      Logging,
      TarantoolError,
      Service
    ] { (cfg, connection) =>
      make(cfg, connection)
    }

  def make(
    cfg: TarantoolConfig,
    connection: TarantoolConnection.Service
  ): ZManaged[Logging, TarantoolError, Service] =
    ZManaged.make(
      for {
        logger <- ZIO.service[Logger[String]]
        queue <- createQueue(cfg.clientConfig.backgroundQueueSize)
        live = new Live(
          logger,
          connection,
          ExecutionContextManager.singleThreaded(),
          queue
        )
        _ <- live.start()
      } yield live
    )(_.close().orDie)

  private def createQueue(size: Int): IO[TarantoolError, Queue[ByteBuffer]] = size match {
    case x if x > 0  => Queue.bounded(x)
    case x if x == 0 => Queue.unbounded
    case x =>
      ZIO.fail(ConfigurationError(s"Configuration 'backgroundQueueSize' has incorrect value: $x"))
  }

  private[tarantool] def requestQueue()
    : ZIO[SocketChannelQueuedWriter, Nothing, Queue[ByteBuffer]] =
    ZIO.access(_.get.requestQueue)

  private[tarantool] class Live(
    logger: Logger[String],
    connection: TarantoolConnection.Service,
    ec: ExecutionContextManager,
    queue: Queue[ByteBuffer]
  ) extends SocketChannelQueuedWriter.Service {

    override def send(packet: MessagePackPacket): IO[TarantoolError, Unit] =
      for {
        buffer <- MessagePackPacket.toBuffer(packet)
        _ <- write(buffer)
      } yield ()

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
      _ <- connection.sendRequest(buffer).flatMap {
        case Some(_) => logger.debug("Successfully write delayed request")
        case None    => logger.debug("Send request to queue again") *> placeToQueue(buffer)
      }
    } yield ()

    private def write(buffer: ByteBuffer): ZIO[Any, TarantoolError, Unit] =
      connection.sendRequest(buffer).flatMap {
        case Some(bytes) => logger.debug(s"[direct write] bytes sent: $bytes")
        case None        => placeToQueue(buffer)
      }

    private def placeToQueue(buffer: ByteBuffer): ZIO[Any, Nothing, Unit] =
      logger.debug("Send request to queue").zipRight(queue.offer(buffer).fork.unit)

    // for testing purposes
    override private[tarantool] val requestQueue = queue
  }
}
