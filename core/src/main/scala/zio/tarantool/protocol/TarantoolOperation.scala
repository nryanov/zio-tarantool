package zio.tarantool.protocol

import zio.{Promise, UIO}
import zio.tarantool.TarantoolError

final case class TarantoolOperation(
  request: TarantoolRequest,
  response: Promise[TarantoolError, TarantoolResponse]
) {
  def isDone: UIO[Boolean] = response.isDone
}

object TarantoolOperation {
  def apply(
    request: TarantoolRequest,
    response: Promise[TarantoolError, TarantoolResponse]
  ): TarantoolOperation = new TarantoolOperation(request, response)
}
