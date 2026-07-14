package zio.tarantool.api

import zio.tarantool.protocol.IteratorCode

private[tarantool] sealed trait BuiltRequest

private[tarantool] object BuiltRequest {
  final case class Select(
    space: SpaceRef,
    index: IndexRef,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpValue
  ) extends BuiltRequest

  final case class Insert(
    space: SpaceRef,
    tuple: MpValue
  ) extends BuiltRequest

  final case class Replace(
    space: SpaceRef,
    tuple: MpValue
  ) extends BuiltRequest

  final case class Delete(
    space: SpaceRef,
    index: IndexRef,
    key: MpValue
  ) extends BuiltRequest

  final case class Update(
    space: SpaceRef,
    index: IndexRef,
    key: MpValue,
    ops: MpValue
  ) extends BuiltRequest

  final case class Upsert(
    space: SpaceRef,
    index: IndexRef,
    ops: MpValue,
    tuple: MpValue
  ) extends BuiltRequest

  final case class Call(
    functionName: String,
    args: MpValue
  ) extends BuiltRequest

  final case class Eval(
    expression: String,
    args: MpValue
  ) extends BuiltRequest

  final case class Execute(
    target: ExecuteTarget,
    sqlBind: MpValue,
    options: MpValue
  ) extends BuiltRequest

  final case class Prepare(
    target: PrepareTarget
  ) extends BuiltRequest

  sealed trait ExecuteTarget
  object ExecuteTarget {
    final case class StatementId(id: Int) extends ExecuteTarget
    final case class Sql(sql: String) extends ExecuteTarget
  }

  sealed trait PrepareTarget
  object PrepareTarget {
    final case class StatementId(id: Int) extends PrepareTarget
    final case class Sql(sql: String) extends PrepareTarget
  }
}
