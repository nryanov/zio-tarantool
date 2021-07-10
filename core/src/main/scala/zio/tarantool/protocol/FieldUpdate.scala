package zio.tarantool.protocol

import zio.tarantool.msgpack.MessagePack

sealed trait FieldUpdate

object FieldUpdate {
  final case class SimpleFieldUpdate(position: Int, operatorCode: OperatorCode, value: MessagePack)
      extends FieldUpdate

  final case class SpliceFieldUpdate(
    position: Int,
    start: Int,
    length: Int,
    operatorCode: OperatorCode,
    replacement: MessagePack
  ) extends FieldUpdate

}
