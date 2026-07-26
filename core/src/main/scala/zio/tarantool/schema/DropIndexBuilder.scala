package zio.tarantool.schema

import _root_.zio.ZIO
import zio.tarantool.TarantoolClient
import zio.tarantool.TarantoolError
import zio.tarantool.schema.internal.{LuaExpr, SchemaOps}

final case class DropIndexBuilder private[schema] (
  private val space: String,
  private val name: String,
  private val ifExists: Boolean = true
) {
  def ifExists(ifExists: Boolean): DropIndexBuilder = copy(ifExists = ifExists)

  def run: ZIO[TarantoolClient.Service, TarantoolError, Unit] =
    SchemaOps.runEval(LuaExpr.dropIndex(space, name, ifExists))
}
