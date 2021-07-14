package zio.tarantool.codec

import org.msgpack.value.Value
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.NotSupportedUpdateOperation
import zio.tarantool.protocol.FieldUpdate.{SimpleFieldUpdate, SpliceFieldUpdate}
import zio.tarantool.protocol.{FieldUpdate, OperatorCode}

sealed trait TupleOps[A] {
  def encoder: Encoder[A]

  // todo: Try ... Catch ?
  def plus(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))

  def minus(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))

  def or(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))

  def and(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))

  def xor(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))

  def splice(
    position: Int,
    start: Int,
    length: Int,
    value: A
  ): Either[TarantoolError, FieldUpdate] =
    Right(SpliceFieldUpdate(position, start, length, OperatorCode.Splice, encoder.encode(value)))

  def insert(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))

  def delete(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))

  def assign(position: Int, value: A): Either[TarantoolError, FieldUpdate] =
    Right(SimpleFieldUpdate(position, OperatorCode.Assigment, encoder.encode(value)))
}

object TupleOps {
  def apply[A](implicit ops: TupleOps[A]): TupleOps[A] = ops

  implicit val messagePackTupleOps: TupleOps[Value] = new TupleOps[Value] {
    override val encoder: Encoder[Value] = Encoder[Value]
  }
  implicit val byteTupleOps: TupleOps[Byte] = new TupleOps[Byte] {
    override val encoder: Encoder[Byte] = Encoder[Byte]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Byte
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }
  implicit val shortTupleOps: TupleOps[Short] = new TupleOps[Short] {
    override val encoder: Encoder[Short] = Encoder[Short]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Short
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }
  implicit val intTupleOps: TupleOps[Int] = new TupleOps[Int] {
    override val encoder: Encoder[Int] = Encoder[Int]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Int
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }
  implicit val longTupleOps: TupleOps[Long] = new TupleOps[Long] {
    override val encoder: Encoder[Long] = Encoder[Long]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Long
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }
  implicit val floatTupleOps: TupleOps[Float] = new TupleOps[Float] {
    override val encoder: Encoder[Float] = Encoder[Float]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Float
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }
  implicit val doubleTupleOps: TupleOps[Double] = new TupleOps[Double] {
    override val encoder: Encoder[Double] = Encoder[Double]
    override def splice(
      position: Int,
      start: Int,
      length: Int,
      value: Double
    ): Either[TarantoolError, FieldUpdate] =
      Left(NotSupportedUpdateOperation("Not supported"))
  }
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
