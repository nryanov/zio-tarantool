package zio.tarantool.protocol

import org.msgpack.value.Value
import zio._
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.{CodecError, EmptyResultSet, ProtocolError}
import zio.tarantool.codec.TupleEncoder

import scala.collection.mutable

sealed trait TarantoolResponse {
  def raw: Value

  def resultSet[A: TupleEncoder]: IO[TarantoolError, Vector[A]]

  final def head[A: TupleEncoder]: IO[TarantoolError, A] =
    headOption.flatMap(opt => IO.require(EmptyResultSet)(IO.succeed(opt)))

  final def headOption[A: TupleEncoder]: IO[TarantoolError, Option[A]] =
    resultSet.map(_.headOption)
}

object TarantoolResponse {
  // Returned data: [tuple]
  final case class TarantoolEvalResponse(messagePack: Value) extends TarantoolResponse {
    override val raw: Value = messagePack

    override def resultSet[A](implicit encoder: TupleEncoder[A]): IO[TarantoolError, Vector[A]] =
      if (messagePack.isArrayValue) {
        val array = messagePack.asArrayValue()
        if (array.size() != 0) {
          IO.effect(encoder.decode(array, 0)).mapBoth(err => CodecError(err), value => Vector(value))
        } else {
          IO.succeed(Vector.empty)
        }
      } else {
        IO.fail(
          ProtocolError(
            s"Unexpected tuple type. Expected MpArray, but got: ${messagePack.getValueType.name()}"
          )
        )
      }
  }

  // Returned data: [ [tuple1], [tuple2], ..., [tupleN] ]
  final case class TarantoolDataResponse(messagePack: Value) extends TarantoolResponse {
    override val raw: Value = messagePack

    override def resultSet[A](implicit encoder: TupleEncoder[A]): IO[TarantoolError, Vector[A]] =
      if (messagePack.isArrayValue) {
        val array = messagePack.asArrayValue()
        val buffer = mutable.ListBuffer[A]()

        IO.effect {
          val iter = array.iterator()

          while (iter.hasNext) {
            val next = iter.next()

            if (next.isArrayValue) {
              val decoded = encoder.decode(next.asArrayValue(), 0)
              buffer.+=(decoded)
            } else {
              throw ProtocolError(
                s"Unexpected tuple type. Expected MpArray, but got: ${next.getValueType.name()}"
              )
            }
          }

          buffer.toVector
        }.mapError(CodecError)
      } else {
        IO.fail(
          ProtocolError(
            s"Unexpected tuple type. Expected MpArray, but got: ${messagePack.getValueType.name()}"
          )
        )
      }

  }
}
