package zio.tarantool.api

import org.msgpack.value.Value
import zio.tarantool.TarantoolError.UpdateOperationError
import zio.tarantool.codec.Encoder
import zio.tarantool.internal.schema.SpaceMeta

import scala.collection.mutable

final case class UpdateQuery(operations: Vector[UpdateOperation]) {
  def encode(spaceMeta: SpaceMeta): Either[Vector[UpdateOperationError], Value] = {
    val (values, errors) = operations.foldLeft((Vector.empty[Value], Vector.empty[UpdateOperationError])) {
      case ((values, errors), operation) =>
        operation.validate(spaceMeta) match {
          case UpdateOperationValidationResult.Success =>
            (values.appended(operation.toQuery()), errors)
          case UpdateOperationValidationResult.Error(reason) =>
            (values, errors.appended(UpdateOperationError(reason)))
        }
    }

    if (errors.nonEmpty) {
      Left(errors)
    } else {
      Right(Encoder[Vector[Value]].encode(values))
    }
  }
}

object UpdateQuery {
  val default: UpdateQuery = UpdateQuery(Vector.empty)

  def builder(): UpdateQueryBuilder = new UpdateQueryBuilder()

  final class UpdateQueryBuilder() {
    private val operations = mutable.ArrayBuffer.empty[UpdateOperation]

    def add(fieldPosition: Int, value: Long): this.type = {
      operations.append(UpdateOperation.Add(fieldPosition, value))
      this
    }

    def subtract(fieldPosition: Int, value: Long): this.type = {
      operations.append(UpdateOperation.Subtract(fieldPosition, value))
      this
    }

    def or(fieldPosition: Int, value: Long): this.type = {
      operations.append(UpdateOperation.Or(fieldPosition, value))
      this
    }

    def and(fieldPosition: Int, value: Long): this.type = {
      operations.append(UpdateOperation.And(fieldPosition, value))
      this
    }

    def xor(fieldPosition: Int, value: Long): this.type = {
      operations.append(UpdateOperation.Xor(fieldPosition, value))
      this
    }

    def splice(fieldPosition: Int, start: Int, length: Int, value: String): this.type = {
      operations.append(UpdateOperation.Splice(fieldPosition, start, length, value))
      this
    }

    def insert[A: Encoder](fieldPosition: Int, value: A): this.type = {
      operations.append(UpdateOperation.Insert(fieldPosition, value))
      this
    }

    def delete(fieldPosition: Int): this.type = {
      operations.append(UpdateOperation.Delete(fieldPosition))
      this
    }

    def set[A: Encoder](fieldPosition: Int, value: A): this.type = {
      operations.append(UpdateOperation.Set(fieldPosition, value))
      this
    }

    def build(): UpdateQuery = UpdateQuery(operations.toVector)

  }
}
