package zio.tarantool.protocol

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import zio.{IO, ZIO}
import scodec.bits.BitVector
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.codec.MessagePackPacketCodec
import zio.tarantool.msgpack.{Encoder, MessagePack, MessagePackCodec}

final case class MessagePackPacket(header: Map[Long, MessagePack], body: Map[Long, MessagePack])

object MessagePackPacket {
  private val InitialRequestSize = 1024

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

  def toBuffer(packet: MessagePackPacket): IO[TarantoolError, ByteBuffer] = for {
    os <- ZIO.effectTotal(new ByteArrayOutputStream(InitialRequestSize))
    encodedPacket <- encodePacket(packet)
    size <- Encoder.longEncoder.encodeM(encodedPacket.bytes.length)
    sizeMp <- encodeMessagePack(size)
    _ <- ZIO.effect(os.write(sizeMp.toByteArray)).refineOrDie(toIOError)
    _ <- ZIO.effect(os.write(encodedPacket.toByteArray)).refineOrDie(toIOError)
  } yield ByteBuffer.wrap(os.toByteArray)

  def extractCode(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    codeMp <- ZIO
      .fromOption(packet.header.get(FieldKey.Code.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no Code in header (${packet.header})")
      )
    codeValue <- Encoder.longEncoder.decodeM(codeMp)
    code <- if (codeValue == 0) ZIO.succeed(codeValue) else extractErrorCode(codeValue)
  } yield code

  def extractError(
    packet: MessagePackPacket
  ): IO[TarantoolError, String] =
    extractByKey(packet, FieldKey.Error).flatMap(Encoder.stringEncoder.decodeM)

  def extractData(
    packet: MessagePackPacket
  ): IO[TarantoolError.ProtocolError, MessagePack] =
    extractByKey(packet, FieldKey.Data)

  def extractSyncId(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    syncIdMp <- ZIO
      .fromOption(packet.header.get(FieldKey.Sync.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SyncId in header (${packet.header})")
      )
    syncId <- Encoder.longEncoder.decodeM(syncIdMp)
  } yield syncId

  def extractSchemaId(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    schemaIdMp <- ZIO
      .fromOption(packet.header.get(FieldKey.SchemaId.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SchemaId in header (${packet.header})")
      )
    schemaId <- Encoder.longEncoder.decodeM(schemaIdMp)
  } yield schemaId

  def extractByKey(
    packet: MessagePackPacket,
    key: FieldKey
  ): ZIO[Any, TarantoolError.ProtocolError, MessagePack] =
    for {
      value <- ZIO
        .fromOption(packet.body.get(key.value))
        .orElseFail(
          TarantoolError.ProtocolError(s"Packet has no $key value in body part ${packet.body}")
        )
    } yield value

  private def extractErrorCode(code: Long): ZIO[Any, TarantoolError.ProtocolError, Long] =
    if ((code & ResponseCode.ErrorTypeMarker.value) == 0) {
      ZIO.fail(TarantoolError.ProtocolError(s"Code $code does not follow 0x8XXX format"))
    } else {
      ZIO.succeed(~ResponseCode.ErrorTypeMarker.value & code)
    }

  private def encodePacket(packet: MessagePackPacket): IO[TarantoolError.CodecError, BitVector] =
    ZIO.effect(MessagePackPacketCodec.encode(packet).require).mapError(TarantoolError.CodecError)

  private def encodeMessagePack(mp: MessagePack): IO[TarantoolError.CodecError, BitVector] =
    IO.effect(MessagePackCodec.encode(mp).require).mapError(TarantoolError.CodecError)
}
