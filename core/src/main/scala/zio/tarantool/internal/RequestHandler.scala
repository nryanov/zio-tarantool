package zio.tarantool.internal

import org.msgpack.value.Value
import zio._
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.TarantoolResponse.{TarantoolDataResponse, TarantoolEvalResponse}
import zio.tarantool.protocol.{RequestCode, TarantoolOperation, TarantoolRequest, TarantoolResponse}

import scala.collection.concurrent.TrieMap

private[tarantool] object RequestHandler {
  type RequestHandler = Has[Service]

  trait Service {
    private[tarantool] def sentRequests: Map[Long, TarantoolOperation]

    def submitRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    def complete(syncId: Long, response: Value): IO[TarantoolError, Unit]

    def fail(syncId: Long, reason: String, errorCode: Int): IO[TarantoolError, Unit]

    def close(): UIO[Unit]
  }

  def submitRequest(
    request: TarantoolRequest
  ): ZIO[RequestHandler, TarantoolError, TarantoolOperation] =
    ZIO.accessM[RequestHandler](_.get.submitRequest(request))

  def complete(
    syncId: Long,
    response: Value
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

  val live: ZLayer[Any, TarantoolError, RequestHandler] = make().toLayer

  def make(): ZManaged[Any, Nothing, Service] =
    ZManaged.make(ZIO.succeed(new Live()))(_.close())

  private[tarantool] def sentRequests: ZIO[RequestHandler, Nothing, Map[Long, TarantoolOperation]] =
    ZIO.access(_.get.sentRequests)

  private[tarantool] class Live() extends Service {
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

    override def complete(syncId: Long, response: Value): IO[TarantoolError, Unit] =
      for {
        operation <- ZIO
          .fromOption(awaitingRequestMap.remove(syncId))
          .orElseFail(TarantoolError.NotFoundOperation(syncId))
        _ <- operation.request.operationCode match {
          case RequestCode.Eval | RequestCode.Call =>
            operation.response.succeed(TarantoolEvalResponse(response))
          case _ => operation.response.succeed(TarantoolDataResponse(response))
        }
      } yield ()

    override def fail(syncId: Long, reason: String, errorCode: Int): IO[TarantoolError, Unit] =
      for {
        operation <- ZIO
          .fromOption(awaitingRequestMap.remove(syncId))
          .orElseFail(TarantoolError.NotFoundOperation(syncId))
        _ <- operation.response.fail(TarantoolError.OperationException(reason, errorCode))
      } yield ()

    override def close(): UIO[Unit] = ZIO
      .foreach_(awaitingRequestMap.values)(op =>
        op.response.fail(TarantoolError.DeclinedOperation(op.request.syncId, op.request.operationCode))
      )
      .zipLeft(IO.effectTotal(awaitingRequestMap.clear()))
  }
}
