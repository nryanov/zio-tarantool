package zio.tarantool.codec

import _root_.zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.CodecError
import zio.tarantool.protocol.{FieldUpdate, UpdateOperations}
import zio.tarantool.codec.TupleOpsBuilder.FieldMeta

import scala.collection.mutable

final class TupleOpsBuilder[C] private[codec] (fields: Map[String, FieldMeta]) {
  private val buffer = mutable.ListBuffer[Either[TarantoolError, FieldUpdate]]()

  def plus[A](field: String, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.plus(meta.position, a))

  def minus[A](field: String, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.minus(meta.position, a))

  def or[A](field: String, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.or(meta.position, a))

  def and[A](field: String, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.and(meta.position, a))

  def xor[A](field: String, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.xor(meta.position, a))

  def splice[A](field: String, start: Int, length: Int, replacement: A)(implicit
    ops: TupleOps[A]
  ): this.type =
    applyOperation(field, replacement, (meta, a) => ops.splice(meta.position, start, length, a))

  def assign[A](field: String, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.assign(meta.position, a))

  def build(): Either[TarantoolError, UpdateOperations] = {
    val attempt: Either[TarantoolError, Vector[FieldUpdate]] = {
      val empty: Either[TarantoolError, Vector[FieldUpdate]] =
        Right[TarantoolError, Vector[FieldUpdate]](Vector.empty[FieldUpdate])

      buffer.foldLeft(empty) { case (acc, el) =>
        acc.flatMap(a => el.map(a :+ _))
      }
    }

    attempt.map(ops => UpdateOperations(ops))
  }

  def buildM(): IO[CodecError, UpdateOperations] =
    ZIO.fromEither(build()).mapError(err => CodecError(err))

  def reset(): Unit = buffer.clear()

  private def applyOperation[A](
    field: String,
    value: A,
    f: (FieldMeta, A) => Either[TarantoolError, FieldUpdate]
  ): this.type = {
    val result: Either[TarantoolError, FieldUpdate] = fields.get(field) match {
      case Some(meta) =>
        f(meta, value).left.map(err =>
          TarantoolError.NotSupportedUpdateOperation(s"$field: ${err.getLocalizedMessage}")
        )
      case None => Left(TarantoolError.NotSupportedUpdateOperation(s"Field $field does not exist"))
    }

    buffer += result
    this
  }
}

object TupleOpsBuilder {
  private[codec] case class FieldMeta(position: Int)

  def apply[A](implicit builder: TupleOpsBuilder[A]): TupleOpsBuilder[A] = builder

  private[codec] def fromFields[A](fields: Map[String, FieldMeta]): TupleOpsBuilder[A] =
    new TupleOpsBuilder[A](fields)
}
