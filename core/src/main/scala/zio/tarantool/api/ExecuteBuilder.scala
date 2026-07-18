package zio.tarantool.api

import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableArrayValueImpl
import _root_.zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

final case class ExecuteBuilder private[api] (
  private val target: Option[BuiltRequest.ExecuteTarget] = None,
  private val sqlBind: MpValue = MpValue.raw(ExecuteBuilder.EmptyTuple),
  private val options: MpValue = MpValue.raw(ExecuteBuilder.EmptyTuple)
) {
  def sql(sql: String): ExecuteBuilder = copy(target = Some(BuiltRequest.ExecuteTarget.Sql(sql)))

  def statementId(statementId: Int): ExecuteBuilder =
    copy(target = Some(BuiltRequest.ExecuteTarget.StatementId(statementId)))

  def bind(sqlBind: Value): ExecuteBuilder = copy(sqlBind = MpValue.raw(sqlBind))

  def bind[A: TupleEncoder](sqlBind: A): ExecuteBuilder = copy(sqlBind = MpValue.typed(sqlBind))

  def options(options: Value): ExecuteBuilder = copy(options = MpValue.raw(options))

  def options[A: TupleEncoder](options: A): ExecuteBuilder = copy(options = MpValue.typed(options))

  def run: ZIO[TarantoolClient.Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      target <- BuilderOps.require(target, "sql or statementId")
      response <- BuilderOps.run(BuiltRequest.Execute(target, sqlBind, options))
    } yield response
}

object ExecuteBuilder {
  private val EmptyTuple = new ImmutableArrayValueImpl(Array.empty)

  def apply(): ExecuteBuilder = new ExecuteBuilder()
}
