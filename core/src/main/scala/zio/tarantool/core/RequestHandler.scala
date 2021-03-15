package zio.tarantool.core

import zio._
import zio.logging._
import zio.macros.accessible
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.{TarantoolOperation, TarantoolRequest, TarantoolResponse}

import scala.collection.concurrent.TrieMap

@accessible[RequestHandler.Service]
object RequestHandler {
  type RequestHandler = Has[Service]

  trait Service {
    private[tarantool] def sentRequests: Map[Long, TarantoolOperation]

    def submitRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    def rescheduleRequest(
      syncId: Long,
      schemaId: Long
    ): IO[TarantoolError, TarantoolOperation]

    def complete(syncId: Long, response: TarantoolResponse): IO[TarantoolError, Unit]

    def fail(syncId: Long, reason: String): IO[TarantoolError, Unit]

    def close(): UIO[Unit]
  }

  val live: ZLayer[Logging, TarantoolError, RequestHandler] = make().toLayer

  def make(): ZManaged[Logging, Nothing, Service] =
    ZManaged.make(ZIO.service[Logger[String]].map(new Live(_)))(_.close())

  private[tarantool] def sentRequests: ZIO[RequestHandler, Nothing, Map[Long, TarantoolOperation]] =
    ZIO.access(_.get.sentRequests)

  private[tarantool] class Live(logger: Logger[String]) extends Service {
    private val awaitingRequestMap: TrieMap[Long, TarantoolOperation] =
      new TrieMap[Long, TarantoolOperation]()

    override private[tarantool] def sentRequests: Map[Long, TarantoolOperation] =
      awaitingRequestMap.toMap

    override def submitRequest(
      request: TarantoolRequest
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        response <- Promise.make[TarantoolError, TarantoolResponse]
        operation = TarantoolOperation(request, response)
        notEmpty <- ZIO.effectTotal(awaitingRequestMap.put(request.syncId, operation).isDefined)
        _ <- ZIO.when(notEmpty)(
          ZIO.fail(
            TarantoolError.OperationException(
              s"Operation with id ${request.syncId} was already sent"
            )
          )
        )
        _ <- logger.debug(s"Operation ${request.syncId} was successfully submitted")
      } yield operation

    override def rescheduleRequest(
      syncId: Long,
      schemaId: Long
    ): IO[TarantoolError, TarantoolOperation] = for {
      operation <- ZIO
        .fromOption(awaitingRequestMap.get(syncId))
        .orElseFail(TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
      newRequest <- TarantoolRequest.withSchemaId(operation.request, schemaId)
      newOperation = operation.copy(request = newRequest)
      empty <- ZIO.effectTotal(awaitingRequestMap.put(syncId, operation).isEmpty)
      _ <- ZIO.when(empty)(
        ZIO.fail(
          TarantoolError.OperationException(
            s"Operation with id $syncId was unexpectedly removed"
          )
        )
      )
    } yield newOperation

    override def complete(syncId: Long, response: TarantoolResponse): IO[TarantoolError, Unit] =
      for {
        operation <- ZIO
          .fromOption(awaitingRequestMap.remove(syncId))
          .orElseFail(TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
        _ <- operation.response.succeed(response)
        _ <- logger.info(s"Response $syncId completed")
      } yield ()

    override def fail(syncId: Long, reason: String): IO[TarantoolError, Unit] = for {
      _ <- logger.info(s"Fail response $syncId due to: $reason")
      operation <- ZIO
        .fromOption(awaitingRequestMap.remove(syncId))
        .orElseFail(TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
      _ <- operation.response.fail(TarantoolError.OperationException(reason))
      _ <- logger.info(s"Response $syncId failed")
    } yield ()

    override def close(): UIO[Unit] = ZIO
      .foreach_(awaitingRequestMap.values)(op =>
        op.response.fail(
          TarantoolError.OperationException(
            s"Operation ${op.request.syncId}:${op.request.operationCode} was declined"
          )
        )
      )
      .zipLeft(IO.effectTotal(awaitingRequestMap.clear()))
  }
}
