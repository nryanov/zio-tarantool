package zio.tarantool.schema

import _root_.zio.ZIO
import zio.tarantool.TarantoolClient
import zio.tarantool.TarantoolError
import zio.tarantool.schema.internal.{LuaExpr, SchemaOps}

final case class DropSpaceBuilder private[schema] (
  private val name: String,
  private val ifExists: Boolean = true
) {
  def ifExists(ifExists: Boolean): DropSpaceBuilder = copy(ifExists = ifExists)

  def run: ZIO[TarantoolClient.Service, TarantoolError, Unit] =
    SchemaOps.runEval(LuaExpr.dropSpace(name, ifExists))
}
