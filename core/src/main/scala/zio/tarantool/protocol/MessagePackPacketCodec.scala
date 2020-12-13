package zio.tarantool.protocol

import scodec.{Attempt, Codec, DecodeResult, Err, SizeBound}
import scodec.bits.BitVector
import shapeless.HNil
import zio.tarantool.msgpack.Codecs.mpMapCodec
import zio.tarantool.msgpack.Encoder.mapEncoder
import zio.tarantool.msgpack.{MessagePack, MpMap}

object MessagePackPacketCodec extends Codec[MessagePackPacket] {
  private val packetMapEncoder = mapEncoder[Long, MessagePack]
  private val mpMapCodecWiden: Codec[MessagePack] = mpMapCodec.widen(
    _.asInstanceOf[MessagePack],
    {
      case map: MpMap => Attempt.successful(map)
      case _          => Attempt.failure(Err("Expected MpMap"))
    }
  )

  private val codec: Codec[MessagePackPacket] =
    (mpMapCodecWiden :: mpMapCodecWiden).xmap[MessagePackPacket](
      msg =>
        MessagePackPacket(
          packetMapEncoder.decodeUnsafe(msg.head),
          packetMapEncoder.decodeUnsafe(msg.tail.head)
        ),
      value =>
        packetMapEncoder.encodeUnsafe(value.header) :: packetMapEncoder.encodeUnsafe(
          value.body
        ) :: HNil
    )

  override def encode(value: MessagePackPacket): Attempt[BitVector] = codec.encode(value)

  override def sizeBound: SizeBound = codec.sizeBound

  override def decode(bits: BitVector): Attempt[DecodeResult[MessagePackPacket]] =
    codec.decode(bits)
}
