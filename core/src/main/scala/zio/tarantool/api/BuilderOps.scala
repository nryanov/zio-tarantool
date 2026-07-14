package zio.tarantool.api

import zio.{IO, Promise, ZIO}
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

private[api] object BuilderOps {
  def require[A](value: Option[A], field: String): IO[TarantoolError, A] =
    IO.fromOption(value).orElseFail(TarantoolError.IncompleteRequest(s"Missing required field: $field"))

  def run(
    request: BuiltRequest
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.execute(request))
}
