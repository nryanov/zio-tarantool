package zio.tarantool.codec

import org.msgpack.value.{ArrayValue, Value}
import shapeless3.deriving.*
import zio.tarantool.TarantoolError.CodecError

import scala.util.{NotGiven, Try}

object auto extends TupleOpsBuilderDerivation:
  extension [A <: Product](value: A) def builder(using opsBuilder: TupleOpsBuilder[A]): TupleOpsBuilder[A] = opsBuilder

  def productEncoder[A](using
    inst: K0.ProductInstances[TupleEncoder, A]
  ): TupleEncoder[A] = new TupleEncoder[A]:
    def encode(v: A): Vector[Value] =
      inst.foldLeft(v)(Vector.empty[Value])(
        [t] => (acc: Vector[Value], enc: TupleEncoder[t], field: t) => Continue(acc ++ enc.encode(field))
      )

    def decode(v: ArrayValue, idx: Int): A =
      val (_, decoded) = inst.unfold[Int](idx)(
        [t] =>
          (i: Int, enc: TupleEncoder[t]) =>
            val value = enc.decode(v, i)
            val width = enc.encode(value).length
            (i + width, Some(value))
      )
      decoded.getOrElse(
        throw CodecError(new IllegalArgumentException(s"Failed to decode product at index $idx"))
      )

  def coproductEncoder[A](using
    inst: K0.CoproductInstances[TupleEncoder, A]
  ): TupleEncoder[A] = new TupleEncoder[A]:
    def encode(v: A): Vector[Value] =
      inst.fold(v)([t] => (enc: TupleEncoder[t], t0: t) => enc.encode(t0))

    def decode(v: ArrayValue, idx: Int): A =
      // Try variants from last to first so more-specific / wider shapes win over narrower ones
      // (e.g. Rect(w,h) before Circle(r) when both could parse a prefix).
      val n = inst.arity
      def go(i: Int): A =
        if i < 0 then throw CodecError(new IllegalArgumentException("Unexpected error while decoding coproduct"))
        else
          Try {
            inst.inject[A](i)([t <: A] => (enc: TupleEncoder[t]) => enc.decode(v, idx))
          }.getOrElse(go(i - 1))
      go(n - 1)

  // Case classes / products only — never Option (handled by TupleEncoder.fromEncoderOption)
  inline implicit def derivedProductEncoder[A](using
    gen: K0.ProductGeneric[A],
    inst: K0.ProductInstances[TupleEncoder, A]
  ): TupleEncoder[A] =
    productEncoder

  // Sealed traits / ADTs, excluding Option[_] (those use fromEncoderOption)
  inline implicit def derivedCoproductEncoder[A](using
    gen: K0.CoproductGeneric[A],
    inst: K0.CoproductInstances[TupleEncoder, A],
    notOption: NotGiven[A <:< Option[Any]]
  ): TupleEncoder[A] =
    coproductEncoder

  implicit def optionProductEncoder[A](using
    enc: TupleEncoder[A],
    ev: A <:< Product
  ): TupleEncoder[Option[A]] = new TupleEncoder[Option[A]]:
    def encode(v: Option[A]): Vector[Value] = v match
      case Some(value) => enc.encode(value)
      case None        => Vector.empty

    def decode(v: ArrayValue, idx: Int): Option[A] =
      if idx >= v.size() then None
      else
        Try(enc.decode(v, idx)).toOption.flatMap { decoded =>
          val fields = enc.encode(decoded)
          if fields.nonEmpty && fields.forall(_.isNilValue) then None
          else if fields.isEmpty then None
          else Some(decoded)
        }
