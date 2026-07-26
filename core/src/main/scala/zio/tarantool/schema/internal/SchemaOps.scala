package zio.tarantool.schema.internal

import org.msgpack.value.impl.ImmutableArrayValueImpl
import _root_.zio.ZIO
import zio.tarantool.api.{BuiltRequest, MpValue}
import zio.tarantool.TarantoolClient
import zio.tarantool.TarantoolError

private[schema] object SchemaOps {
  private val EmptyTuple = MpValue.raw(new ImmutableArrayValueImpl(Array.empty))

  def runEval(expression: String): ZIO[TarantoolClient.Service, TarantoolError, Unit] =
    for {
      promise <- ZIO.serviceWithZIO[TarantoolClient.Service](
        _.execute(BuiltRequest.Eval(expression, EmptyTuple))
      )
      _ <- promise.await.unit
      _ <- ZIO.serviceWithZIO[TarantoolClient.Service](_.refreshMeta())
    } yield ()
}
