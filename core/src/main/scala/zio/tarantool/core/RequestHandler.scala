package zio.tarantool.core

import zio._
import zio.logging._
import zio.macros.accessible
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.{TarantoolOperation, TarantoolRequest, TarantoolResponse}

import scala.collection.concurrent.TrieMap

@accessible[RequestHandler.Service]
private[tarantool] object RequestHandler {
  type RequestHandler = Has[Service]

  trait Service {
    private[tarantool] def sentRequests: Map[Long, TarantoolOperation]

    def submitRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    def complete(syncId: Long, response: TarantoolResponse): IO[TarantoolError, Unit]

    def fail(syncId: Long, reason: String, errorCode: Int): IO[TarantoolError, Unit]

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
        _ <- ZIO.when(notEmpty)(ZIO.fail(TarantoolError.DuplicateOperation(request.syncId)))
      } yield operation

    override def complete(syncId: Long, response: TarantoolResponse): IO[TarantoolError, Unit] =
      for {
        operation <- ZIO
          .fromOption(awaitingRequestMap.remove(syncId))
          .orElseFail(TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
        _ <- operation.response.succeed(response)
      } yield ()

    override def fail(syncId: Long, reason: String, errorCode: Int): IO[TarantoolError, Unit] =
      for {
        operation <- ZIO
          .fromOption(awaitingRequestMap.remove(syncId))
          .orElseFail(TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
        _ <- operation.response.fail(TarantoolError.OperationException(reason, errorCode))
      } yield ()

    override def close(): UIO[Unit] = ZIO
      .foreach_(awaitingRequestMap.values)(op =>
        op.response
          .fail(TarantoolError.DeclinedOperation(op.request.syncId, op.request.operationCode))
      )
      .zipLeft(IO.effectTotal(awaitingRequestMap.clear()))
  }
}
