package zio.tarantool.codec

import shapeless._
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import zio.IO
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.CodecError
import zio.tarantool.protocol.{FieldUpdate, UpdateOperations}
import zio.tarantool.codec.TupleOpsBuilder.FieldMeta

import scala.collection.mutable

final class TupleOpsBuilder[C] private (fields: Map[String, FieldMeta]) {
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

  // fixme
  def build(): Either[TarantoolError, UpdateOperations] =
//    val attempt: Either[TarantoolError, Vector[FieldUpdate]] =
//      buffer.foldLeft(Either[TarantoolError, Vector[FieldUpdate]](Vector.empty[FieldUpdate])) {
//        case (acc, el) =>
//          acc.flatMap(a => el.map(a :+ _))
//      }
//
//    attempt.map(ops => UpdateOperations(ops))
    ???

  def buildM(): IO[CodecError, UpdateOperations] = ???
//    IO.effect(build().require).mapError(err => CodecError(err))

  def reset(): Unit = buffer.clear()

  private def applyOperation[A](
    field: String,
    value: A,
    f: (FieldMeta, A) => Either[TarantoolError, FieldUpdate]
  ): this.type =
//    val result: Either[TarantoolError, FieldUpdate] = fields.get(field) match {
//      case Some(meta) => f(meta, value).mapErr(err => Err(s"$field: ${err.message}"))
//      case None       => Attempt.failure(Err(s"Field $field does not exist"))
//    }
//
//    buffer += result
//    this
    ???
}

object TupleOpsBuilder {
  private[tarantool] case class FieldMeta(position: Int)

  def apply[A](implicit builder: TupleOpsBuilder[A]): TupleOpsBuilder[A] = builder

  implicit def newBuilder[A, ARepr <: HList, KeysRepr <: HList](implicit
    gen: LabelledGeneric.Aux[A, ARepr],
    keys: Keys.Aux[ARepr, KeysRepr],
    keysToTraversable: ToTraversable.Aux[KeysRepr, List, Symbol]
  ): TupleOpsBuilder[A] = {
    val fieldMetas: Map[String, FieldMeta] =
      keys().toList.zipWithIndex.map { case (symbol, i) =>
        symbol.name -> FieldMeta(i)
      }.toMap

    new TupleOpsBuilder[A](fieldMetas)
  }
}
