package zio.tarantool.protocol

import org.msgpack.core.MessageUnpacker
import org.msgpack.value.Value
import zio.tarantool.codec.{Encoder, TupleEncoder}

final case class UpdateOperations(ops: Vector[FieldUpdate])

object UpdateOperations {
  implicit val updateOperationsTupleEncoder: TupleEncoder[UpdateOperations] =
    new TupleEncoder[UpdateOperations] {
      override def encode(v: UpdateOperations): Value = {
        val encodedOps: Vector[Value] =
          v.ops.foldLeft(Vector.empty[Value]) { (state, ops) =>
            val opsEncoded: Value = ops match {
              case FieldUpdate.SimpleFieldUpdate(position, operatorCode, value) =>
                val positionEncoded = Encoder[Int].encode(position)
                val operationEncoded = Encoder[OperatorCode].encode(operatorCode)

                Encoder[Vector[Value]].encode(Vector(operationEncoded, positionEncoded, value))
              case FieldUpdate.SpliceFieldUpdate(
                    position,
                    start,
                    length,
                    operatorCode,
                    replacement
                  ) =>
                val positionEncoded = Encoder[Int].encode(position)
                val startEncoded = Encoder[Int].encode(start)
                val lengthEncoded = Encoder[Int].encode(length)
                val operationEncoded = Encoder[OperatorCode].encode(operatorCode)

                Encoder[Vector[Value]].encode(
                  Vector(
                    operationEncoded,
                    positionEncoded,
                    startEncoded,
                    lengthEncoded,
                    replacement
                  )
                )
            }

            state :+ opsEncoded
          }

        Encoder[Vector[Value]].encode(encodedOps)
      }

      override def decode(unpacker: MessageUnpacker): UpdateOperations =
        throw new UnsupportedOperationException("UpdateOperations decoding is not supported")
    }
}
