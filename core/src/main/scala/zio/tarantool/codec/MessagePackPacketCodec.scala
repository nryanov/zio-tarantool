package zio.tarantool.codec

import org.msgpack.core.MessageUnpacker
import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableArrayValueImpl
import zio.tarantool.protocol.MessagePackPacket

object MessagePackPacketCodec extends TupleEncoder[MessagePackPacket] {

  override def encode(v: MessagePackPacket): Vector[Value] =
    Vector(
      new ImmutableArrayValueImpl(
        Array(
          Encoder[Map[Long, Value]].encode(v.header),
          Encoder[Map[Long, Value]].encode(v.body)
        )
      )
    )

  override def decode(unpacker: MessageUnpacker): MessagePackPacket = {
    val value = unpacker.unpackValue()
    val map = value.asArrayValue().iterator()

    val header = Encoder[Map[Long, Value]].decode(map.next())
    val body = Encoder[Map[Long, Value]].decode(map.next())

    MessagePackPacket(header, body)
  }
}
