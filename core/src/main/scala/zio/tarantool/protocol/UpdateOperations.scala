package zio.tarantool.protocol

import scodec.{Attempt, Err}
import zio.tarantool.msgpack._
import zio.tarantool.codec.TupleEncoder

final case class UpdateOperations(ops: Vector[FieldUpdate])

object UpdateOperations {
  implicit val updateOperationsTupleEncoder: TupleEncoder[UpdateOperations] =
    new TupleEncoder[UpdateOperations] {
      override def encode(v: UpdateOperations): Attempt[MpArray] = {
        val encodedOps: Attempt[Vector[MessagePack]] =
          v.ops.foldLeft(Attempt.successful(Vector.empty[MpArray])) { (state, ops) =>
            val opsEncoded: Attempt[MpFixArray] = ops match {
              case FieldUpdate.SimpleFieldUpdate(position, operatorCode, value) =>
                for {
                  positionEncoded <- Encoder[Int].encode(position)
                  operationEncoded <- Encoder[OperatorCode].encode(operatorCode)
                } yield MpFixArray(Vector(operationEncoded, positionEncoded, value))
              case FieldUpdate.SpliceFieldUpdate(
                    position,
                    start,
                    length,
                    operatorCode,
                    replacement
                  ) =>
                for {
                  positionEncoded <- Encoder[Int].encode(position)
                  startEncoded <- Encoder[Int].encode(start)
                  lengthEncoded <- Encoder[Int].encode(length)
                  operationEncoded <- Encoder[OperatorCode].encode(operatorCode)
                } yield MpFixArray(
                  Vector(
                    operationEncoded,
                    positionEncoded,
                    startEncoded,
                    lengthEncoded,
                    replacement
                  )
                )
            }

            for {
              s <- state
              op <- opsEncoded
            } yield s :+ op
          }

        encodedOps.map { vector =>
          val len = vector.size

          if (len <= 15) MpFixArray(vector)
          else if (len <= 65535) MpArray16(vector)
          else MpArray32(vector)
        }
      }

      override def decode(v: MpArray, idx: Int): Attempt[UpdateOperations] =
        Attempt.failure(Err("Not supported"))
    }
}
