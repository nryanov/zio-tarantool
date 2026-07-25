package zio.tarantool.schema.internal

import zio.tarantool.schema.IndexPart.{ByName, ByPosition}
import zio.tarantool.schema.{FieldFormat, IndexPart}

private[schema] object LuaExpr {

  def createSpace(
    name: String,
    engine: Option[String],
    temporary: Option[Boolean],
    ifNotExists: Option[Boolean],
    format: List[FieldFormat]
  ): String = {
    val formatOpt =
      if (format.nonEmpty) Some(s"format = ${renderFormat(format)}") else None
    val opts = List(
      engine.map(e => s"engine = ${str(e)}"),
      temporary.map(t => s"temporary = $t"),
      ifNotExists.map(v => s"if_not_exists = $v"),
      formatOpt
    ).flatten

    val optsPart = if (opts.isEmpty) "" else s", {${opts.mkString(", ")}}"
    s"box.schema.create_space(${str(name)}$optsPart)"
  }

  def createIndex(
    space: String,
    name: String,
    indexType: Option[String],
    unique: Option[Boolean],
    ifNotExists: Option[Boolean],
    sequence: Option[String],
    parts: List[IndexPart]
  ): String = {
    val partsOpt =
      if (parts.nonEmpty) Some(s"parts = {${parts.map(renderPart).mkString(", ")}}") else None
    val opts = List(
      indexType.map(t => s"type = ${str(t)}"),
      unique.map(u => s"unique = $u"),
      ifNotExists.map(v => s"if_not_exists = $v"),
      sequence.map(s => s"sequence = ${str(s)}"),
      partsOpt
    ).flatten

    val optsPart = if (opts.isEmpty) "" else s", {${opts.mkString(", ")}}"
    s"box.space[${str(space)}]:create_index(${str(name)}$optsPart)"
  }

  def dropSpace(name: String, ifExists: Boolean): String =
    if (ifExists)
      s"if box.space[${str(name)}] then box.space[${str(name)}]:drop() end"
    else
      s"box.space[${str(name)}]:drop()"

  def dropIndex(space: String, name: String, ifExists: Boolean): String =
    if (ifExists)
      s"if box.space[${str(space)}] and box.space[${str(space)}].index[${str(name)}] then " +
        s"box.space[${str(space)}].index[${str(name)}]:drop() end"
    else
      s"box.space[${str(space)}].index[${str(name)}]:drop()"

  def truncate(space: String): String =
    s"box.space[${str(space)}]:truncate()"

  def str(value: String): String =
    "'" + value
      .replace("\\", "\\\\")
      .replace("'", "\\'")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t") + "'"

  private def renderFormat(format: List[FieldFormat]): String =
    format.map { f =>
      val nullable = if (f.isNullable) ", is_nullable = true" else ""
      s"{name = ${str(f.name)}, type = ${str(f.fieldType)}$nullable}"
    }.mkString("{", ", ", "}")

  private def renderPart(part: IndexPart): String =
    part match {
      case ByPosition(field, fieldType) => s"{$field, ${str(fieldType)}}"
      case ByName(field, fieldType)     => s"{${str(field)}, ${str(fieldType)}}"
    }
}
