package zio.tarantool.protocol

import zio.tarantool.msgpack.Encoder

sealed abstract class OperatorCode(val value: String)

/**
 * "+" for addition. values must be numeric, e.g. unsigned or decimal
 * "-" for subtraction. values must be numeric
 * "&" for bitwise AND. values must be unsigned numeric
 * "|" for bitwise OR. values must be unsigned numeric
 * "^^" for bitwise XOR. values must be unsigned numeric
 * ":" for string splice.
 * "!" for insertion of a new field.
 * "#" for deletion.
 * "=" for assignment.
 */
object OperatorCode {
  case object Addition extends OperatorCode("+")
  case object Subtraction extends OperatorCode("-")
  case object Or extends OperatorCode("|")
  case object And extends OperatorCode("&")
  case object Xor extends OperatorCode("^")
  case object Splice extends OperatorCode(":")
  case object Insertion extends OperatorCode("!")
  case object Deletion extends OperatorCode("#")
  case object Assigment extends OperatorCode("=")

  implicit val operatorCodeEncoder: Encoder[OperatorCode] =
    Encoder.stringEncoder.xmap(
      _ => throw new UnsupportedOperationException("OperatorCode decoding is not supported"),
      _.value
    )
}
