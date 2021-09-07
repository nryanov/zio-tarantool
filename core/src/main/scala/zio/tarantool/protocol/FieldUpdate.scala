package zio.tarantool.protocol

import org.msgpack.value.Value

sealed trait FieldUpdate

object FieldUpdate {
  final case class SimpleFieldUpdate(position: Int, operatorCode: OperatorCode, value: Value) extends FieldUpdate

  final case class SpliceFieldUpdate(
    position: Int,
    start: Int,
    length: Int,
    operatorCode: OperatorCode,
    replacement: Value
  ) extends FieldUpdate

}
