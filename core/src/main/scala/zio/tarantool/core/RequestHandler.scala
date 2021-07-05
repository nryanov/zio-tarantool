package zio.tarantool.core

import zio._
import zio.logging._
import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.protocol.TarantoolResponse.{TarantoolDataResponse, TarantoolEvalResponse}
import zio.tarantool.protocol.{RequestCode, TarantoolOperation, TarantoolRequest, TarantoolResponse}

import scala.collection.concurrent.TrieMap

private[tarantool] object RequestHandler {
  type RequestHandler = Has[Service]

  trait Service {
    private[tarantool] def sentRequests: Map[Long, TarantoolOperation]

    def submitRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    def complete(syncId: Long, response: MessagePack): IO[TarantoolError, Unit]

    def fail(syncId: Long, reason: String, errorCode: Int): IO[TarantoolError, Unit]

    def close(): UIO[Unit]
  }

  def submitRequest(
    request: TarantoolRequest
  ): ZIO[RequestHandler, TarantoolError, TarantoolOperation] =
    ZIO.accessM[RequestHandler](_.get.submitRequest(request))

  def complete(
    syncId: Long,
    response: MessagePack
  ): ZIO[RequestHandler, TarantoolError, Unit] =
    ZIO.accessM[RequestHandler](_.get.complete(syncId, response))

  def fail(
    syncId: Long,
    reason: String,
    errorCode: Int
  ): ZIO[RequestHandler, TarantoolError, Unit] =
    ZIO.accessM[RequestHandler](_.get.fail(syncId, reason, errorCode))

  def close(
  ): ZIO[RequestHandler, Nothing, Unit] =
    ZIO.accessM[RequestHandler](_.get.close())

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

    override def complete(syncId: Long, response: MessagePack): IO[TarantoolError, Unit] =
      for {
        operation <- ZIO
          .fromOption(awaitingRequestMap.remove(syncId))
          .orElseFail(TarantoolError.NotFoundOperation(s"Operation $syncId not found"))
        _ <- operation.request.operationCode match {
          case RequestCode.Eval => // todo: Call ?
            operation.response.succeed(TarantoolEvalResponse(response))
          case _ => operation.response.succeed(TarantoolDataResponse(response))
        }
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
