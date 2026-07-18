package zio.tarantool.codec

import org.msgpack.value.{ArrayValue, Value}
import shapeless3.deriving.*
import zio.tarantool.TarantoolError.CodecError

import scala.util.Try

object auto extends TupleOpsBuilderDerivation:
  extension [A <: Product](value: A)
    def builder(using opsBuilder: TupleOpsBuilder[A]): TupleOpsBuilder[A] = opsBuilder

  given productEncoder[A](using
    inst: K0.ProductInstances[TupleEncoder, A]
  ): TupleEncoder[A] with
    def encode(v: A): Vector[Value] =
      inst.foldLeft(v)(Vector.empty[Value])(
        [t] =>
          (acc: Vector[Value], enc: TupleEncoder[t], field: t) =>
            Continue(acc ++ enc.encode(field))
      )

    def decode(v: ArrayValue, idx: Int): A =
      val (_, decoded) = inst.unfold[Int](idx)(
        [t] =>
          (i: Int, enc: TupleEncoder[t]) =>
            (i + 1, Some(enc.decode(v, i)))
      )
      decoded.getOrElse(
        throw CodecError(new IllegalArgumentException(s"Failed to decode product at index $idx"))
      )

  given coproductEncoder[A](using
    inst: K0.CoproductInstances[TupleEncoder, A]
  ): TupleEncoder[A] with
    def encode(v: A): Vector[Value] =
      inst.fold(v)([t] => (enc: TupleEncoder[t], t0: t) => enc.encode(t0))

    def decode(v: ArrayValue, idx: Int): A =
      val n = inst.arity
      def go(i: Int): A =
        if i >= n then
          throw CodecError(new IllegalArgumentException("Unexpected error while decoding coproduct"))
        else
          Try {
            inst.inject[A](i)([t <: A] => (enc: TupleEncoder[t]) => enc.decode(v, idx))
          }.getOrElse(go(i + 1))
      go(0)

  given optionProductEncoder[A](using
    enc: TupleEncoder[A],
    ev: A <:< Product
  ): TupleEncoder[Option[A]] with
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
