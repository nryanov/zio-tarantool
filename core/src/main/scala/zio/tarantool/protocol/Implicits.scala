package zio.tarantool.protocol

import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.codec.{Encoder, TupleEncoder}
import org.msgpack.core.MessagePack
import org.msgpack.value.Value

private[tarantool] object Implicits {
  implicit class RichEncoder[A](encoder: Encoder[A]) {
    def encodeM(v: A): IO[TarantoolError.CodecError, Value] =
      ZIO.effect(encoder.encode(v)).mapError(TarantoolError.CodecError)

    def decodeM(value: Value): IO[TarantoolError.CodecError, A] =
      ZIO.effect(encoder.decode(value)).mapError(TarantoolError.CodecError)
  }

  implicit class RichTupleEncoder[A](encoder: TupleEncoder[A]) {
    def encodeM(v: A): IO[TarantoolError.CodecError, Value] =
      ZIO.effect(encoder.encode(v)).mapBoth(TarantoolError.CodecError, Encoder[Vector[Value]].encode)

    def decodeM(v: Value): IO[TarantoolError.CodecError, A] =
      ZIO.effect(encoder.decode(v.asArrayValue(), 0)).mapError(TarantoolError.CodecError)

    def deserialize(v: Array[Byte]): IO[TarantoolError.CodecError, A] =
      ZIO.effect {
        val unpacker = MessagePack.newDefaultUnpacker(v)
        val nextValue = unpacker.unpackValue()

        if (!nextValue.isArrayValue) {
          throw new IllegalArgumentException(s"Expected ArrayType, but got: $nextValue")
        }

        encoder.decode(nextValue.asArrayValue(), 0)
      }.mapError(TarantoolError.CodecError)
  }

  implicit class RichValue(val value: Value) {
    def serialize(): IO[TarantoolError.CodecError, Array[Byte]] =
      IO.effect {
        val packer = MessagePack.newDefaultBufferPacker()
        packer.packValue(value)
        packer.close()
        packer.toByteArray
      }.mapError(TarantoolError.CodecError)
  }
}
