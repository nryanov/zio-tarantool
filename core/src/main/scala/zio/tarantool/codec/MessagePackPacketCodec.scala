package zio.tarantool.codec

import org.msgpack.value.{ArrayValue, Value}
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

  override def decode(v: ArrayValue, idx: Int): MessagePackPacket = {
    val header = Encoder[Map[Long, Value]].decode(v.get(idx))
    val body = Encoder[Map[Long, Value]].decode(v.get(idx + 1))

    MessagePackPacket(header, body)
  }

}
