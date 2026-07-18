package zio.tarantool.api

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableArrayValueImpl
import _root_.zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

final case class CallBuilder private[api] (
  private val functionName: Option[String] = None,
  private val args: MpValue = MpValue.raw(CallBuilder.EmptyTuple)
) {
  def function(functionName: String): CallBuilder = copy(functionName = Some(functionName))

  def args(args: Value): CallBuilder = copy(args = MpValue.raw(args))

  def args[A: TupleEncoder](args: A): CallBuilder = copy(args = MpValue.typed(args))

  def run: ZIO[TarantoolClient.Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      functionName <- BuilderOps.require(functionName, "function")
      response <- BuilderOps.run(BuiltRequest.Call(functionName, args))
    } yield response
}

object CallBuilder {
  private val EmptyTuple = new ImmutableArrayValueImpl(Array.empty)

  def apply(): CallBuilder = new CallBuilder()
}
