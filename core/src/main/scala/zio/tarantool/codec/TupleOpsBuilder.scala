package zio.tarantool.codec

import scodec.{Attempt, Err}
import shapeless._
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import zio.IO
import zio.tarantool.TarantoolError.CodecError
import zio.tarantool.protocol.{FieldUpdate, UpdateOperations}
import zio.tarantool.codec.TupleOpsBuilder.FieldMeta

import scala.collection.mutable

final class TupleOpsBuilder[C] private (fields: Map[Symbol, FieldMeta]) {
  private val buffer = mutable.ListBuffer[Attempt[FieldUpdate]]()

  def plus[A](field: Symbol, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.plus(meta.position, a))

  def minus[A](field: Symbol, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.minus(meta.position, a))

  def or[A](field: Symbol, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.or(meta.position, a))

  def and[A](field: Symbol, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.and(meta.position, a))

  def xor[A](field: Symbol, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.xor(meta.position, a))

  def splice[A](field: Symbol, start: Int, length: Int, replacement: A)(implicit
    ops: TupleOps[A]
  ): this.type =
    applyOperation(field, replacement, (meta, a) => ops.splice(meta.position, start, length, a))

  def assign[A](field: Symbol, value: A)(implicit ops: TupleOps[A]): this.type =
    applyOperation(field, value, (meta, a) => ops.assign(meta.position, a))

  def build(): Attempt[UpdateOperations] = {
    val attempt: Attempt[Vector[FieldUpdate]] =
      buffer.foldLeft(Attempt.successful(Vector.empty[FieldUpdate])) { case (acc, el) =>
        acc.flatMap(a => el.map(a :+ _))
      }

    attempt.map(ops => UpdateOperations(ops))
  }

  def buildM(): IO[CodecError, UpdateOperations] =
    IO.effect(build().require).mapError(err => CodecError(err))

  def reset(): Unit = buffer.clear()

  private def applyOperation[A](
    field: Symbol,
    value: A,
    f: (FieldMeta, A) => Attempt[FieldUpdate]
  ): this.type = {
    val result: Attempt[FieldUpdate] = fields.get(field) match {
      case Some(meta) => f(meta, value).mapErr(err => Err(s"$field: ${err.message}"))
      case None       => Attempt.failure(Err(s"Field $field does not exist"))
    }

    buffer += result
    this
  }
}

object TupleOpsBuilder {
  private[tarantool] case class FieldMeta(position: Int)

  def apply[A](implicit builder: TupleOpsBuilder[A]): TupleOpsBuilder[A] = builder

  implicit def newBuilder[A, ARepr <: HList, KeysRepr <: HList](implicit
    gen: LabelledGeneric.Aux[A, ARepr],
    keys: Keys.Aux[ARepr, KeysRepr],
    keysToTraversable: ToTraversable.Aux[KeysRepr, List, Symbol]
  ): TupleOpsBuilder[A] = {
    val fieldMetas: Map[Symbol, FieldMeta] =
      keys().toList.zipWithIndex.map { case (symbol, i) =>
        symbol -> FieldMeta(i)
      }.toMap

    new TupleOpsBuilder[A](fieldMetas)
  }
}
