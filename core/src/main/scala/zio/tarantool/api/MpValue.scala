package zio.tarantool.api

import org.msgpack.value.Value
import _root_.zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.Implicits._

private[tarantool] sealed trait MpValue {
  def encode: IO[TarantoolError, Value]
}

private[tarantool] object MpValue {
  def raw(value: Value): MpValue = Raw(value)

  def typed[A: TupleEncoder](value: A): MpValue = Typed(value, implicitly[TupleEncoder[A]])

  private final case class Raw(value: Value) extends MpValue {
    override def encode: IO[TarantoolError, Value] = ZIO.succeed(value)
  }

  private final case class Typed[A](value: A, encoder: TupleEncoder[A]) extends MpValue {
    override def encode: IO[TarantoolError, Value] = encoder.encodeM(value)
  }
}
