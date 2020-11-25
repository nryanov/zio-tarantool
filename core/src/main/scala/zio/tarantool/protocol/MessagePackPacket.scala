package zio.tarantool.protocol

import scodec.Codec
import shapeless.HNil
import zio.tarantool.msgpack.{MpFixMap, MpMap}
import zio.tarantool.msgpack.Codecs.mpMapCodec

final case class MessagePackPacket(header: MpMap, body: MpMap)

object MessagePackPacket {
  implicit val messagePackPacketCodec: Codec[MessagePackPacket] =
    (mpMapCodec :: mpMapCodec)
      .xmap[MessagePackPacket](msg => MessagePackPacket(msg.head, msg.tail.head), value => value.header :: value.body :: HNil)

  def apply(header: MpMap, body: Option[MpMap]): MessagePackPacket = body match {
    case Some(value) => MessagePackPacket(header, value)
    case None        => MessagePackPacket(header)
  }

  def apply(header: MpMap, body: MpMap): MessagePackPacket = new MessagePackPacket(header, body)

  def apply(header: MpMap): MessagePackPacket = new MessagePackPacket(header, MpFixMap(Map.empty))
}
