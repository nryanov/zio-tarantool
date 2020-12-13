package zio.tarantool.protocol

import zio.tarantool.msgpack.MessagePack

final case class MessagePackPacket(header: Map[Long, MessagePack], body: Map[Long, MessagePack])

object MessagePackPacket {
  def apply(
    header: Map[Long, MessagePack],
    body: Option[Map[Long, MessagePack]]
  ): MessagePackPacket = body match {
    case Some(value) => MessagePackPacket(header, value)
    case None        => MessagePackPacket(header)
  }

  def apply(header: Map[Long, MessagePack], body: Map[Long, MessagePack]): MessagePackPacket =
    new MessagePackPacket(header, body)

  def apply(header: Map[Long, MessagePack]): MessagePackPacket =
    new MessagePackPacket(header, Map.empty)
}
