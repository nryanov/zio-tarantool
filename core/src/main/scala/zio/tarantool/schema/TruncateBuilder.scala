package zio.tarantool.schema

import _root_.zio.ZIO
import zio.tarantool.TarantoolClient
import zio.tarantool.TarantoolError
import zio.tarantool.schema.internal.{LuaExpr, SchemaOps}

final case class TruncateBuilder private[schema] (private val space: String) {
  def run: ZIO[TarantoolClient.Service, TarantoolError, Unit] =
    SchemaOps.runEval(LuaExpr.truncate(space))
}
