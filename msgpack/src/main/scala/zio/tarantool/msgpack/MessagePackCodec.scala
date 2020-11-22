package zio.tarantool.msgpack

import scodec.{Attempt, Codec, DecodeResult, SizeBound}
import scodec.bits._
import scodec.codecs._
import Codecs._

object MessagePackCodec extends Codec[MessagePack] {
  private val codec: Codec[MessagePack] = lazily(Codec.coproduct[MessagePack].choice)

  override def encode(value: MessagePack): Attempt[BitVector] = codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound

  override def decode(bits: BitVector): Attempt[DecodeResult[MessagePack]] = codec.decode(bits)
}
