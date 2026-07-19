package zio.tarantool.internal

import org.msgpack.value.Value
import _root_.zio._
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.TarantoolResponse.{TarantoolDataResponse, TarantoolEvalResponse}
import zio.tarantool.protocol.{RequestCode, TarantoolOperation, TarantoolRequest, TarantoolResponse}

import scala.collection.concurrent.TrieMap

private[tarantool] object RequestHandler {
  trait Service {
    private[tarantool] def sentRequests: Map[Long, TarantoolOperation]

    def submitRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    def complete(syncId: Long, response: Value): IO[TarantoolError, Unit]

    def fail(syncId: Long, reason: String, errorCode: Int): IO[TarantoolError, Unit]

    def close(): UIO[Unit]
  }

  def submitRequest(
    request: TarantoolRequest
  ): ZIO[Service, TarantoolError, TarantoolOperation] =
    ZIO.serviceWithZIO(_.submitRequest(request))

  def complete(
    syncId: Long,
    response: Value
  ): ZIO[Service, TarantoolError, Unit] =
    ZIO.serviceWithZIO(_.complete(syncId, response))

  def fail(
    syncId: Long,
    reason: String,
    errorCode: Int
  ): ZIO[Service, TarantoolError, Unit] =
    ZIO.serviceWithZIO(_.fail(syncId, reason, errorCode))

  def close(): ZIO[Service, Nothing, Unit] =
    ZIO.serviceWithZIO(_.close())

  val live: ZLayer[Any, Nothing, Service] =
    ZLayer.scoped(make())

  def make(): ZIO[Scope, Nothing, Service] =
    ZIO.acquireRelease(ZIO.succeed(new Live()))(_.close())

  private[tarantool] def sentRequests: ZIO[Service, Nothing, Map[Long, TarantoolOperation]] =
    ZIO.serviceWith(_.sentRequests)

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
        notEmpty <- ZIO.succeed(awaitingRequestMap.put(request.syncId, operation).isDefined)
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
      .foreachDiscard(awaitingRequestMap.values)(op =>
        op.response.fail(TarantoolError.DeclinedOperation(op.request.syncId, op.request.operationCode))
      )
      .zipLeft(ZIO.succeed(awaitingRequestMap.clear()))
  }
}
