package zio.tarantool.internal.impl

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import scodec.bits.{BitVector, ByteVector}
import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.codec.MessagePackPacketCodec
import zio.tarantool.internal.PacketManager
import zio.tarantool.internal.impl.PacketManagerLive._
import zio.tarantool.msgpack.{Encoder, MessagePack, MessagePackCodec}
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.{Code, Key, MessagePackPacket, OperationCode}

import scala.collection.mutable

private[tarantool] final class PacketManagerLive extends PacketManager.Service {
  override def decodeToMessagePack(v: ByteVector): IO[TarantoolError.CodecError, MessagePack] =
    IO.effect(MessagePackCodec.decodeValue(v.toBitVector).require)
      .mapError(TarantoolError.CodecError)

  override def decodeToMessagePackPacket(
    vector: ByteVector
  ): IO[TarantoolError.CodecError, MessagePackPacket] = IO
    .effect(
      MessagePackPacketCodec.decodeValue(vector.toBitVector).require
    )
    .mapError(TarantoolError.CodecError)

  override def createPacket(
    op: OperationCode,
    syncId: Long,
    schemaId: Option[Long],
    body: Map[Long, MessagePack]
  ): IO[TarantoolError.CodecError, MessagePackPacket] = {
    val headerMp = mutable.Map[Long, MessagePack]()

    for {
      _ <- numberEncoder.encodeM(syncId).map(mp => headerMp += Key.Sync.value -> mp)
      _ <- numberEncoder.encodeM(op.value).map(mp => headerMp += Key.Code.value -> mp)
      _ <- ZIO
        .fromOption(schemaId)
        .tap(id => numberEncoder.encodeM(id).map(mp => headerMp += Key.Code.value -> mp))
        .orElse(ZIO.unit)
    } yield MessagePackPacket(headerMp.toMap, body)
  }

  override def toBuffer(packet: MessagePackPacket): IO[TarantoolError, ByteBuffer] = for {
    os <- ZIO.effectTotal(new ByteArrayOutputStream(InitialRequestSize))
    encodedPacket <- encodePacket(packet)
    size <- numberEncoder.encodeM(encodedPacket.bytes.length)
    sizeMp <- encodeMessagePack(size)
    _ <- ZIO.effect(os.write(sizeMp.toByteArray)).refineOrDie(toIOError)
    _ <- ZIO.effect(os.write(encodedPacket.toByteArray)).refineOrDie(toIOError)
  } yield ByteBuffer.wrap(os.toByteArray)

  override def extractCode(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    codeMp <- ZIO
      .fromOption(packet.header.get(Key.Code.value))
      .orElseFail(TarantoolError.ProtocolError(s"Packet has no Code in header (${packet.header})"))
    codeValue <- numberEncoder.decodeM(codeMp)
    code <- if (codeValue == 0) ZIO.succeed(codeValue) else extractErrorCode(codeValue)
  } yield code

  override def extractError(
    packet: MessagePackPacket
  ): IO[TarantoolError, String] =
    extractByKey(packet, Key.Error).flatMap(stringEncoder.decodeM)

  override def extractData(
    packet: MessagePackPacket
  ): IO[TarantoolError.ProtocolError, MessagePack] =
    extractByKey(packet, Key.Data)

  override def extractSyncId(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    syncIdMp <- ZIO
      .fromOption(packet.header.get(Key.Sync.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SyncId in header (${packet.header})")
      )
    syncId <- numberEncoder.decodeM(syncIdMp)
  } yield syncId

  override def extractSchemaId(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    schemaIdMp <- ZIO
      .fromOption(packet.header.get(Key.SchemaId.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SchemaId in header (${packet.header})")
      )
    schemaId <- numberEncoder.decodeM(schemaIdMp)
  } yield schemaId

  private def extractByKey(
    packet: MessagePackPacket,
    key: Key
  ): ZIO[Any, TarantoolError.ProtocolError, MessagePack] =
    for {
      value <- ZIO
        .fromOption(packet.body.get(key.value))
        .orElseFail(
          TarantoolError.ProtocolError(s"Packet has no $key value in body part ${packet.body}")
        )
    } yield value

  private def extractErrorCode(code: Long): ZIO[Any, TarantoolError.ProtocolError, Long] =
    if ((code & Code.ErrorTypeMarker.value) == 0) {
      ZIO.fail(TarantoolError.ProtocolError(s"Code $code does not follow 0x8XXX format"))
    } else {
      ZIO.succeed(~Code.ErrorTypeMarker.value & code)
    }

  private def encodeMessagePack(mp: MessagePack): IO[TarantoolError.CodecError, BitVector] =
    IO.effect(MessagePackCodec.encode(mp).require).mapError(TarantoolError.CodecError)

  private def encodePacket(packet: MessagePackPacket): IO[TarantoolError.CodecError, BitVector] =
    ZIO.effect(MessagePackPacketCodec.encode(packet).require).mapError(TarantoolError.CodecError)
}

private[tarantool] object PacketManagerLive {
  private val InitialRequestSize = 1024

  private val numberEncoder: Encoder[Long] = Encoder.longEncoder
  private val stringEncoder: Encoder[String] = Encoder.stringEncoder
}
