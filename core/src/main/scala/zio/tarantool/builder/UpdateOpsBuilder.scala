package zio.tarantool.builder

import scodec.Attempt
import zio.tarantool.msgpack._
import zio.tarantool.protocol.{OperationCode, OperatorCode}

import scala.collection.mutable

/* Field number goes from 0 */
final class UpdateOpsBuilder extends Builder[Attempt[MpArray]] {
  private val buffer = mutable.ListBuffer[Attempt[MessagePack]]()

  def add[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Addition)
    this
  }

  def subtract[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Subtraction)
    this
  }

  def or[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Or)
    this
  }

  def and[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.And)
    this
  }

  def xor[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Xor)
    this
  }

  def splice[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Splice)
    this
  }

  def insert[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Insertion)
    this
  }

  def delete[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Deletion)
    this
  }

  def set[A](fieldNumber: Int, value: A)(implicit encoder: Encoder[A]): UpdateOpsBuilder = {
    buffer += encode(fieldNumber, value, OperatorCode.Assigment)
    this
  }

  def build(): Attempt[MpArray] = {
    val attempt: Attempt[Vector[MessagePack]] =
      buffer.foldLeft(Attempt.successful(Vector.empty[MessagePack])) { case (acc, el) =>
        acc.flatMap(a => el.map(a :+ _))
      }

    attempt.map { vector =>
      val len = vector.size

      if (len <= 15) MpFixArray(vector)
      else if (len <= 65535) MpArray16(vector)
      else MpArray32(vector)
    }
  }

  private def encode[A](fieldNumber: Int, value: A, operator: OperatorCode)(implicit
    encoder: Encoder[A]
  ): Attempt[MessagePack] = {
    val op: Attempt[MessagePack] = Encoder[String].encode(operator.value)
    val number: Attempt[MessagePack] = Encoder[Int].encode(fieldNumber)
    val encodedValue: Attempt[MessagePack] = encoder.encode(value)

    val encodedOp = for {
      f1 <- op
      f2 <- number
      f3 <- encodedValue
      updateOp <- Encoder[Vector[MessagePack]].encode(Vector(f1, f2, f3))
    } yield updateOp

    encodedOp
  }
}

object UpdateOpsBuilder {
  def apply(): UpdateOpsBuilder = new UpdateOpsBuilder()
}
