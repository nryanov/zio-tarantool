package zio.tarantool.protocol

import zio.{Promise, UIO}

final case class TarantoolOperation(
  request: TarantoolRequest,
  response: Promise[Throwable, TarantoolResponse]
) {
  def isDone: UIO[Boolean] = response.isDone
}

object TarantoolOperation {
  def apply(
    request: TarantoolRequest,
    response: Promise[Throwable, TarantoolResponse]
  ): TarantoolOperation = new TarantoolOperation(request, response)
}
