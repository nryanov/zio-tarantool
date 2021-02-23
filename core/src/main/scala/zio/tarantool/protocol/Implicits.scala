package zio.tarantool.protocol

import scodec.bits.{BitVector, ByteVector}

import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
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

  private[tarantool] implicit class RichMessagePack(v: MessagePack) {

    /** Used for getting message size */
    final def toNumber: Long = v match {
      case MpPositiveFixInt(value) => value
      case MpUint8(value)          => value
      case MpUint16(value)         => value
      case MpUint32(value)         => value
      case MpUint64(value)         => value
      case MpInt8(value)           => value
      case MpInt16(value)          => value
      case MpInt32(value)          => value
      case MpInt64(value)          => value
      case MpNegativeFixInt(value) => value
      case v                       => throw UnexpectedMessagePackType(s"Not a natural number: ${v.typeName()}")
    }

    final def toMap: MpMap = v match {
      case v: MpMap => v
      case v        => throw UnexpectedMessagePackType(s"Type: ${v.typeName()}")
    }
  }

  private[tarantool] implicit class RichByteVector(v: ByteVector) {
    def decodeM(): IO[TarantoolError.CodecError, MessagePack] =
      IO.effect(MessagePackCodec.decodeValue(v.toBitVector).require)
        .mapError(TarantoolError.CodecError)
  }

  private[tarantool] implicit class RichBitVector(v: BitVector) {
    def decodeM(): IO[TarantoolError.CodecError, MessagePack] =
      IO.effect(MessagePackCodec.decodeValue(v).require).mapError(TarantoolError.CodecError)
  }
}
