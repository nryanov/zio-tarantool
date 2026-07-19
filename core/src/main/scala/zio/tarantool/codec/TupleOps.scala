package zio.tarantool.codec

import org.msgpack.value.Value
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.NotSupportedUpdateOperation
import zio.tarantool.protocol.FieldUpdate.{SimpleFieldUpdate, SpliceFieldUpdate}
import zio.tarantool.protocol.{FieldUpdate, OperatorCode}

sealed trait TupleOps[A] {
  def encoder: Encoder[A]

  def plus(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Left(NotSupportedUpdateOperation(s"plus is not supported at position $position"))

  def minus(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Left(NotSupportedUpdateOperation(s"minus is not supported at position $position"))

  def or(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Left(NotSupportedUpdateOperation(s"or is not supported at position $position"))

  def and(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Left(NotSupportedUpdateOperation(s"and is not supported at position $position"))

  def xor(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Left(NotSupportedUpdateOperation(s"xor is not supported at position $position"))

  def splice(
    position: Int,
    start: Int,
    length: Int,
    value: A
  ): Either[TarantoolError, FieldUpdate] =
    Right(SpliceFieldUpdate(position, start, length, OperatorCode.Splice, encoder.encode(value)))

  def insert(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Insertion, encoder.encode(value)))

  def delete(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Deletion, encoder.encode(value)))

  def assign(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))
}

object TupleOps {
  def apply[A](implicit ops: TupleOps[A]): TupleOps[A] = ops

  implicit val messagePackTupleOps: TupleOps[Value] = new TupleOps[Value] {
    override val encoder: Encoder[Value] = Encoder[Value]
  }
  private def numericTupleOps[A](enc: Encoder[A]): TupleOps[A] = new TupleOps[A] {
    override val encoder: Encoder[A] = enc

    override def plus(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
      Right(SimpleFieldUpdate(position, OperatorCode.Addition, encoder.encode(value)))

    override def minus(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
      Right(SimpleFieldUpdate(position, OperatorCode.Subtraction, encoder.encode(value)))

    override def or(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
      Right(SimpleFieldUpdate(position, OperatorCode.Or, encoder.encode(value)))

    override def and(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
      Right(SimpleFieldUpdate(position, OperatorCode.And, encoder.encode(value)))

    override def xor(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
      Right(SimpleFieldUpdate(position, OperatorCode.Xor, encoder.encode(value)))

    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: A
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }

  implicit val byteTupleOps: TupleOps[Byte] = numericTupleOps(Encoder[Byte])
  implicit val shortTupleOps: TupleOps[Short] = numericTupleOps(Encoder[Short])
  implicit val intTupleOps: TupleOps[Int] = numericTupleOps(Encoder[Int])
  implicit val longTupleOps: TupleOps[Long] = numericTupleOps(Encoder[Long])
  implicit val floatTupleOps: TupleOps[Float] = numericTupleOps(Encoder[Float])
  implicit val doubleTupleOps: TupleOps[Double] = numericTupleOps(Encoder[Double])
  implicit val booleanTupleOps: TupleOps[Boolean] = new TupleOps[Boolean] {
    override val encoder: Encoder[Boolean] = Encoder[Boolean]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Boolean
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }

  implicit val characterTupleOps: TupleOps[Char] = new TupleOps[Char] {
    override val encoder: Encoder[Char] = Encoder[Char]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Char
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }
  implicit val stringTupleOps: TupleOps[String] = new TupleOps[String] {

    override val encoder: Encoder[String] = Encoder[String]

    override def plus(position: Int, value: String): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def minus(position: Int, value: String): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def or(position: Int, value: String): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def and(position: Int, value: String): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def xor(position: Int, value: String): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }

  implicit def vectorTupleOps[A: Encoder]: TupleOps[Vector[A]] = new TupleOps[Vector[A]] {
    override val encoder: Encoder[Vector[A]] = Encoder[Vector[A]]

    override def plus(position: Int, value: Vector[A]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def minus(position: Int, value: Vector[A]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def or(position: Int, value: Vector[A]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def and(position: Int, value: Vector[A]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def xor(position: Int, value: Vector[A]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Vector[A]
    ): Either[TarantoolError, FieldUpdate] = Left(NotSupportedUpdateOperation("Not supported"))
  }

  implicit def mapTupleOps[A: Encoder, B: Encoder]: TupleOps[Map[A, B]] = new TupleOps[Map[A, B]] {
    override val encoder: Encoder[Map[A, B]] = Encoder[Map[A, B]]

    override def plus(position: Int, value: Map[A, B]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def minus(position: Int, value: Map[A, B]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def or(position: Int, value: Map[A, B]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def and(position: Int, value: Map[A, B]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def xor(position: Int, value: Map[A, B]): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))

    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Map[A, B]
    ): Either[TarantoolError, FieldUpdate] = Left(NotSupportedUpdateOperation("Not supported"))
  }
}
