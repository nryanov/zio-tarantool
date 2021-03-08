package zio.tarantool.core

import zio._
import zio.logging._
import zio.macros.accessible
import zio.internal.Executor
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.SchemaMetaManager.SchemaMetaManager

@accessible[DelayedQueue.Service]
object DelayedQueue {
  type DelayedQueue = Has[Service]

  trait Service {
    def reschedule(syncId: Long, newSchemaId: Long): IO[TarantoolError, Unit]

    def start(): UIO[Fiber.Runtime[Throwable, Nothing]]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  final case class RescheduledOperation(syncId: Long, newSchemaId: Long)

  val live: ZLayer[
    SchemaMetaManager with RequestHandler with Logging,
    TarantoolError.IOError,
    DelayedQueue
  ] =
    ZLayer.fromServicesManaged[
      SchemaMetaManager.Service,
      RequestHandler.Service,
      Logging,
      TarantoolError.IOError,
      Service
    ]((schemaMetaManager, requestHandler) => make(schemaMetaManager, requestHandler))

  val test = ZLayer.succeed(new Service {
    override def reschedule(syncId: Long, newSchemaId: Long): IO[TarantoolError, Unit] = IO.unit

    override def start(): UIO[Fiber.Runtime[Throwable, Nothing]] =
      IO.die(new NotImplementedError("Not implemented"))

    override def close(): IO[TarantoolError.IOError, Unit] = IO.unit
  })

  def make(
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: RequestHandler.Service
  ): ZManaged[Logging, TarantoolError.IOError, Service] =
    ZManaged.make(
      for {
        logger <- ZIO.service[Logger[String]]
        queue <- Queue.unbounded[RescheduledOperation]
        live = new Live(
          logger,
          schemaMetaManager,
          requestHandler,
          ExecutionContextManager.singleThreaded(),
          queue
        )
        _ <- live.start()
      } yield live
    )(_.close().orDie)

  private[tarantool] class Live(
    logger: Logger[String],
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: RequestHandler.Service,
    ec: ExecutionContextManager,
    queue: Queue[RescheduledOperation]
  ) extends Service {

    override def reschedule(syncId: Long, newSchemaId: Long): IO[TarantoolError, Unit] =
      logger.info(s"Reschedule: $syncId, schema: $newSchemaId") *>
        IO.ifM(queue.offer(RescheduledOperation(syncId, newSchemaId)))(
          IO.unit,
          IO.fail(TarantoolError.OperationException(s"Could not reschedule operation: $syncId"))
        )

    override def start(): UIO[Fiber.Runtime[Throwable, Nothing]] =
      start0()
        .tapError(err => logger.error(s"Error happened in background worker: $err"))
        .forever
        .lock(Executor.fromExecutionContext(1000)(ec.executionContext))
        .fork

    override def close(): ZIO[Any, TarantoolError.IOError, Unit] =
      for {
        _ <- logger.debug("Shutdown DelayedQueue")
        _ <- queue.shutdown
        _ <- logger.debug("Stop DelayedQueue thread pool")
        _ <- ec.shutdown().refineOrDie(toIOError)
      } yield ()

    private def start0(): ZIO[Any, Throwable, Unit] = for {
      size <- queue.size
      _ <- logger.debug(s"Queue size: $size. Wait new delayed requests")
      delayed <- queue.take
      cachedSchemaId <- schemaMetaManager.schemaId
      _ <- ZIO.when(delayed.newSchemaId > cachedSchemaId)(schemaMetaManager.refresh)
      _ <- requestHandler.rescheduleRequest(delayed.syncId, delayed.newSchemaId)
    } yield ()
  }
}
