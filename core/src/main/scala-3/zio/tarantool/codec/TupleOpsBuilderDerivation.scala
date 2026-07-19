package zio.tarantool.codec

import shapeless3.deriving.Labelling
import zio.tarantool.codec.TupleOpsBuilder.FieldMeta

private[codec] trait TupleOpsBuilderDerivation:
  // Use implicit def so `import auto._` brings it into scope (Scala 3 wildcard skips givens)
  implicit def tupleOpsBuilder[A](using labelling: Labelling[A]): TupleOpsBuilder[A] =
    val fieldMetas: Map[String, FieldMeta] =
      labelling.elemLabels.zipWithIndex.map { case (name, i) =>
        name -> FieldMeta(i)
      }.toMap

    TupleOpsBuilder.fromFields[A](fieldMetas)
