package zio.tarantool.schema

import _root_.zio.ZIO
import zio.tarantool.TarantoolClient
import zio.tarantool.TarantoolError
import zio.tarantool.schema.internal.{LuaExpr, SchemaOps}

final case class CreateSpaceBuilder private[schema] (
  private val name: String,
  private val engine: Option[String] = None,
  private val temporary: Option[Boolean] = None,
  private val ifNotExists: Option[Boolean] = None,
  private val format: List[FieldFormat] = Nil
) {
  def engine(engine: String): CreateSpaceBuilder = copy(engine = Some(engine))

  def temporary(temporary: Boolean): CreateSpaceBuilder = copy(temporary = Some(temporary))

  def ifNotExists(ifNotExists: Boolean): CreateSpaceBuilder = copy(ifNotExists = Some(ifNotExists))

  def format(fields: FieldFormat*): CreateSpaceBuilder = copy(format = fields.toList)

  def format(fields: List[FieldFormat]): CreateSpaceBuilder = copy(format = fields)

  def run: ZIO[TarantoolClient.Service, TarantoolError, Unit] =
    SchemaOps.runEval(
      LuaExpr.createSpace(name, engine, temporary, ifNotExists, format)
    )
}
