package zio.tarantool.api

import org.msgpack.value.Value
import zio.tarantool.codec.Encoder
import zio.tarantool.internal.schema._
import zio.tarantool.protocol.OperatorCode

sealed trait UpdateOperation {
  def operator: OperatorCode

  // todo: Check field types
  def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult

  def toQuery(): Value
}

object UpdateOperation {

  final case class Add(fieldPosition: Int, value: Long) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Addition

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[Long].encode(value)
        )
      )
  }

  final case class Subtract(fieldPosition: Int, value: Long) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Subtraction

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[Long].encode(value)
        )
      )
  }

  final case class Or(fieldPosition: Int, value: Long) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Or

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[Long].encode(value)
        )
      )
  }

  final case class And(fieldPosition: Int, value: Long) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.And

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[Long].encode(value)
        )
      )
  }

  final case class Xor(fieldPosition: Int, value: Long) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Xor

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[Long].encode(value)
        )
      )
  }

  final case class Splice(fieldPosition: Int, start: Int, length: Int, value: String) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Splice

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[Int].encode(start),
          Encoder[Int].encode(length),
          Encoder[String].encode(value)
        )
      )
  }

  final case class Insert[A: Encoder](fieldPosition: Int, value: A) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Insertion

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[A].encode(value)
        )
      )
  }

  final case class Delete(fieldPosition: Int) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Deletion

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition)
        )
      )
  }

  final case class Set[A: Encoder](fieldPosition: Int, value: A) extends UpdateOperation {
    override val operator: OperatorCode = OperatorCode.Assigment

    override def validate(spaceMeta: SpaceMeta): UpdateOperationValidationResult =
      if (fieldPosition >= spaceMeta.fieldFormat.length) {
        UpdateOperationValidationResult.Error(s"Space ${spaceMeta.spaceName} has no field at position $fieldPosition")
      } else {
        UpdateOperationValidationResult.Success
      }

    override def toQuery(): Value =
      Encoder[Vector[Value]].encode(
        Vector(
          Encoder[OperatorCode].encode(operator),
          Encoder[Int].encode(fieldPosition),
          Encoder[A].encode(value)
        )
      )
  }

}
