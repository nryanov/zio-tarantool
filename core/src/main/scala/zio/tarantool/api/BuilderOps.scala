package zio.tarantool.api

import _root_.zio.{IO, Promise, ZIO}
import zio.tarantool.TarantoolClient
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

private[api] object BuilderOps {
  def require[A](value: Option[A], field: String): IO[TarantoolError, A] =
    ZIO.fromOption(value).orElseFail(TarantoolError.IncompleteRequest(s"Missing required field: $field"))

  def run(
    request: BuiltRequest
  ): ZIO[TarantoolClient.Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.serviceWithZIO[TarantoolClient.Service](_.execute(request))
}
