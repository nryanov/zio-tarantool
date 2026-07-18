package zio.tarantool.codec

import shapeless3.deriving.Labelling
import zio.tarantool.codec.TupleOpsBuilder.FieldMeta

private[codec] trait TupleOpsBuilderDerivation:
  given [A](using labelling: Labelling[A]): TupleOpsBuilder[A] =
    val fieldMetas: Map[String, FieldMeta] =
      labelling.elemLabels.zipWithIndex.map { case (name, i) =>
        name -> FieldMeta(i)
      }.toMap

    TupleOpsBuilder.fromFields[A](fieldMetas)
