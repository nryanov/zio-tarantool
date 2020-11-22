package zio.tarantool.protocol

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import scodec.Codec
import shapeless.HNil
import zio._
import zio.tarantool.msgpack.{MpFixMap, MpMap}
import zio.tarantool.msgpack.Implicits._
import zio.tarantool.msgpack.Codecs.mpMapCodec
import zio.tarantool.msgpack.Encoder.longEncoder
import MessagePackPacket._

final case class MessagePackPacket(
  header: MpMap,
  body: MpMap
) {

  def toBuffer: ZIO[Any, Throwable, ByteBuffer] = for {
    os <- ZIO.effectTotal(new ByteArrayOutputStream(InitialRequestSize))
    encodedHeader <- ZIO.effect(header.encode().require)
    encodedBody <- ZIO.effect(body.encode().require)
    size <- ZIO.effect(longEncoder.encodeUnsafe(encodedBody.bytes.length + encodedHeader.bytes.length))
    _ <- ZIO.effect(os.writeBytes(size.encode().require.toByteArray))
    _ <- ZIO.effect(os.writeBytes(encodedHeader.toByteArray))
    _ <- ZIO.effect(os.writeBytes(encodedBody.toByteArray))
  } yield ByteBuffer.wrap(os.toByteArray)
}

object MessagePackPacket {
  private val InitialRequestSize = 1024

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
