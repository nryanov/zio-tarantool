package zio.tarantool.api

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableArrayValueImpl
import _root_.zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

final case class EvalBuilder private[api] (
  private val expression: Option[String] = None,
  private val args: MpValue = MpValue.raw(EvalBuilder.EmptyTuple)
) {
  def expression(expression: String): EvalBuilder = copy(expression = Some(expression))

  def args(args: Value): EvalBuilder = copy(args = MpValue.raw(args))

  def args[A: TupleEncoder](args: A): EvalBuilder = copy(args = MpValue.typed(args))

  def run: ZIO[TarantoolClient.Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      expression <- BuilderOps.require(expression, "expression")
      response <- BuilderOps.run(BuiltRequest.Eval(expression, args))
    } yield response
}

object EvalBuilder {
  private val EmptyTuple = new ImmutableArrayValueImpl(Array.empty)

  def apply(): EvalBuilder = new EvalBuilder()
}
