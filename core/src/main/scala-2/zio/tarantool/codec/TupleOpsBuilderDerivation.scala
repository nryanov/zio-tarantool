package zio.tarantool.codec

import shapeless._
import shapeless.ops.hlist.ToTraversable
import shapeless.ops.record.Keys
import zio.tarantool.codec.TupleOpsBuilder.FieldMeta

private[codec] trait TupleOpsBuilderDerivation {
  implicit def newBuilder[A, ARepr <: HList, KeysRepr <: HList](implicit
    gen: LabelledGeneric.Aux[A, ARepr],
    keys: Keys.Aux[ARepr, KeysRepr],
    keysToTraversable: ToTraversable.Aux[KeysRepr, List, Symbol]
  ): TupleOpsBuilder[A] = {
    val fieldMetas: Map[String, FieldMeta] =
      keys().toList.zipWithIndex.map { case (symbol, i) =>
        symbol.name -> FieldMeta(i)
      }.toMap

    TupleOpsBuilder.fromFields[A](fieldMetas)
  }
}
