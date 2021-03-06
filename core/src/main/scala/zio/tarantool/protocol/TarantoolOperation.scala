package zio.tarantool.protocol

import zio.tarantool.TarantoolError
import zio.{Promise, UIO}

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
