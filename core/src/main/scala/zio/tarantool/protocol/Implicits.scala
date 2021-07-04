package zio.tarantool.protocol

import scodec.bits.{BitVector, ByteVector}
import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.msgpack._
import zio.tarantool.msgpack.MessagePackException.UnexpectedMessagePackType

object Implicits {
  private[tarantool] implicit class RichEncoder[A](encoder: Encoder[A]) {
    def encodeM(v: A): IO[TarantoolError.CodecError, MessagePack] =
      ZIO.effect(encoder.encode(v).require).mapError(TarantoolError.CodecError)

    def decodeM(v: MessagePack): IO[TarantoolError.CodecError, A] =
      ZIO.effect(encoder.decode(v).require).mapError(TarantoolError.CodecError)
  }

  private[tarantool] implicit class RichTupleEncoder[A](encoder: TupleEncoder[A]) {
    def encodeM(v: A): IO[TarantoolError.CodecError, MpArray] =
      ZIO.effect(encoder.encode(v).require).mapError(TarantoolError.CodecError)

    def decodeM(v: MpArray, idx: Int): IO[TarantoolError.CodecError, A] =
      ZIO.effect(encoder.decodeUnsafe(v, idx)).mapError(TarantoolError.CodecError)
  }

  private[tarantool] implicit class RichByteVector(v: ByteVector) {
    def decodeM(): IO[TarantoolError.CodecError, MessagePack] =
      IO.effect(MessagePackCodec.decodeValue(v.toBitVector).require)
        .mapError(TarantoolError.CodecError)

    def decodeUnsafe(): MessagePack =
      MessagePackCodec.decodeValue(v.toBitVector).require
  }
}
