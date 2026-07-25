package zio.tarantool.schema

import _root_.zio.ZIO
import zio.tarantool.TarantoolClient
import zio.tarantool.TarantoolError
import zio.tarantool.schema.internal.{LuaExpr, SchemaOps}

final case class CreateIndexBuilder private[schema] (
  private val space: String,
  private val name: String,
  private val indexType: Option[String] = None,
  private val unique: Option[Boolean] = None,
  private val ifNotExists: Option[Boolean] = None,
  private val sequence: Option[String] = None,
  private val parts: List[IndexPart] = Nil
) {
  def indexType(indexType: String): CreateIndexBuilder = copy(indexType = Some(indexType))

  def unique(unique: Boolean): CreateIndexBuilder = copy(unique = Some(unique))

  def ifNotExists(ifNotExists: Boolean): CreateIndexBuilder = copy(ifNotExists = Some(ifNotExists))

  def sequence(sequence: String): CreateIndexBuilder = copy(sequence = Some(sequence))

  def parts(parts: IndexPart*): CreateIndexBuilder = copy(parts = parts.toList)

  def parts(parts: List[IndexPart]): CreateIndexBuilder = copy(parts = parts)

  def run: ZIO[TarantoolClient.Service, TarantoolError, Unit] =
    SchemaOps.runEval(
      LuaExpr.createIndex(space, name, indexType, unique, ifNotExists, sequence, parts)
    )
}
