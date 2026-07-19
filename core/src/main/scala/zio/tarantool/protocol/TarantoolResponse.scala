package zio.tarantool.protocol

import org.msgpack.value.Value
import _root_.zio._
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.{CodecError, EmptyResultSet, ProtocolError}
import zio.tarantool.codec.TupleEncoder

import scala.collection.mutable

sealed trait TarantoolResponse {
  def raw: Value

  def resultSet[A: TupleEncoder]: IO[TarantoolError, Vector[A]]

  final def head[A: TupleEncoder]: IO[TarantoolError, A] =
    headOption.flatMap {
      case Some(value) => ZIO.succeed(value)
      case None        => ZIO.fail(EmptyResultSet)
    }

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
          ZIO.attempt(encoder.decode(array, 0)).mapBoth(err => CodecError(err), value => Vector(value))
        } else {
          ZIO.succeed(Vector.empty)
        }
      } else {
        ZIO.fail(
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

        ZIO.attempt {
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
        ZIO.fail(
          ProtocolError(
            s"Unexpected tuple type. Expected MpArray, but got: ${messagePack.getValueType.name()}"
          )
        )
      }

  }
}
