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
    def submitRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    def complete(syncId: Long, response: TarantoolResponse): IO[TarantoolError, Unit]

    def fail(syncId: Long, reason: String): IO[TarantoolError, Unit]

    def close(): UIO[Unit]
  }

  val live: ZLayer[Logging, TarantoolError, RequestHandler] = make().toLayer

  def make(): ZManaged[Logging, Nothing, Service] =
    ZManaged.make(ZIO.service[Logger[String]].map(new Live(_)))(_.close())

  private[this] final class Live(logger: Logger[String]) extends Service {
    private val awaitingRequestMap: TrieMap[Long, TarantoolOperation] =
      new TrieMap[Long, TarantoolOperation]()

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

    override def complete(syncId: Long, response: TarantoolResponse): IO[TarantoolError, Unit] =
      for {
        operation <- ZIO
          .fromOption(awaitingRequestMap.remove(syncId))
          .orDieWith(_ => TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
        _ <- operation.response.succeed(response)
      } yield ()

    override def fail(syncId: Long, reason: String): IO[TarantoolError, Unit] = for {
      operation <- ZIO
        .fromOption(awaitingRequestMap.remove(syncId))
        .orDieWith(_ => TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
      _ <- operation.response.fail(TarantoolError.OperationException(reason))
    } yield ()

    override def close(): UIO[Unit] = ZIO.foreach_(awaitingRequestMap.values)(op =>
      op.response.fail(
        TarantoolError.OperationException(
          s"Operation ${op.request.syncId}:${op.request.operationCode} was declined"
        )
      )
    )
  }
}
