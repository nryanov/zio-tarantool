package zio.tarantool.protocol

import zio._
import scodec.{Attempt, Err}
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.{CodecError, EmptyResultSet, ProtocolError}
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.msgpack.{MessagePack, MpArray}

sealed trait TarantoolResponse {
  def resultSet[A: TupleEncoder]: IO[TarantoolError, Vector[A]]

  final def head[A: TupleEncoder]: IO[TarantoolError, A] =
    headOption.flatMap(opt => IO.require(EmptyResultSet)(IO.succeed(opt)))

  final def headOption[A: TupleEncoder]: IO[TarantoolError, Option[A]] =
    resultSet.map(_.headOption)
}

object TarantoolResponse {
  // Returned data: [tuple]
  final case class TarantoolEvalResponse(messagePack: MessagePack) extends TarantoolResponse {
    override def resultSet[A](implicit encoder: TupleEncoder[A]): IO[TarantoolError, Vector[A]] =
      messagePack match {
        case v: MpArray =>
          if (v.value.nonEmpty) {
            IO.effect(encoder.decode(v, 0).require)
              .bimap(err => CodecError(err), value => Vector(value))
          } else {
            IO.succeed(Vector.empty)
          }
        case v =>
          IO.fail(
            ProtocolError(s"Unexpected tuple type. Expected MpArray, but got: ${v.typeName()}")
          )
      }
  }

  // Returned data: [ [tuple1], [tuple2], ..., [tupleN] ]
  final case class TarantoolDataResponse(messagePack: MessagePack) extends TarantoolResponse {
    override def resultSet[A](implicit encoder: TupleEncoder[A]): IO[TarantoolError, Vector[A]] =
      messagePack match {
        case v: MpArray =>
          IO.effect(
            v.value
              .foldLeft(Attempt.successful(Vector.empty[A])) {
                case (acc, value: MpArray) =>
                  for {
                    a <- acc
                    decodedValue <- encoder.decode(value, 0)
                  } yield a :+ decodedValue
                case (_, value) =>
                  Attempt.failure(
                    Err(s"Unexpected tuple type. Expected MpArray, but got: ${value.typeName()}")
                  )
              }
              .require
          ).mapError(CodecError)
        case v =>
          IO.fail(
            ProtocolError(s"Unexpected tuple type. Expected MpArray, but got: ${v.typeName()}")
          )
      }
  }
}
