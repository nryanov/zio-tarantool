package zio.tarantool.impl

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import zio.{Task, ZIO}
import zio.tarantool.PacketManager
import zio.tarantool.msgpack.Implicits._
import zio.tarantool.msgpack.{Encoder, MessagePack, MpFixMap, MpMap}
import zio.tarantool.protocol.{Code, Key, MessagePackPacket, OperationCode}

import scala.collection.mutable
import PacketManagerLive._
import zio.tarantool.msgpack.MessagePackException.MessagePackEncodingException

import scala.util.control.NoStackTrace

class PacketManagerLive extends PacketManager.Service {
  override def createPacket(
    op: OperationCode,
    syncId: Long,
    schemaId: Option[Long],
    body: MpMap
  ): ZIO[Any, Throwable, MessagePackPacket] = {
    val headerMp = mutable.Map[MessagePack, MessagePack]()

    for {
      _ <- ZIO.effect(headerMp += numberEncoder.encodeUnsafe(Key.Sync.value) -> numberEncoder.encodeUnsafe(syncId))
      _ <- ZIO.effect(headerMp += numberEncoder.encodeUnsafe(Key.Code.value) -> numberEncoder.encodeUnsafe(op.value))
      _ <- ZIO.effect(schemaId.foreach(id => headerMp += numberEncoder.encodeUnsafe(Key.SchemaId.value) -> numberEncoder.encodeUnsafe(id)))
    } yield MessagePackPacket(MpFixMap(headerMp.toMap), body)
  }

  override def toBuffer(packet: MessagePackPacket): ZIO[Any, Throwable, ByteBuffer] = for {
    os <- ZIO.effectTotal(new ByteArrayOutputStream(InitialRequestSize))
    encodedHeader <- ZIO.effect(packet.header.encode().require)
    encodedBody <- ZIO.effect(packet.body.encode().require)
    size <- ZIO.effect(numberEncoder.encodeUnsafe(encodedBody.bytes.length + encodedHeader.bytes.length))
    _ <- ZIO.effect(os.writeBytes(size.encode().require.toByteArray))
    _ <- ZIO.effect(os.writeBytes(encodedHeader.toByteArray))
    _ <- ZIO.effect(os.writeBytes(encodedBody.toByteArray))
  } yield ByteBuffer.wrap(os.toByteArray)

  override def extractCode(packet: MessagePackPacket): ZIO[Any, Throwable, Long] = for {
    header <- decodeToMap(packet.header)
    codeMp <- ZIO.fromOption(header.get(Key.Code.value)).mapError(_ => IncorrectPacket("Packet has no Code in header", packet))
    codeValue <- ZIO.effect(numberEncoder.decodeUnsafe(codeMp)).mapError(MessagePackEncodingException)
    code <- if (codeValue == 0) ZIO.succeed(codeValue) else extractErrorCode(codeValue)
  } yield code

  override def extractError(packet: MessagePackPacket): ZIO[Any, Throwable, String] =
    extractByKey(packet, Key.Error).map(stringEncoder.decodeUnsafe)

  override def extractData(packet: MessagePackPacket): ZIO[Any, Throwable, MessagePack] = extractByKey(packet, Key.Data)

  private def extractByKey(packet: MessagePackPacket, key: Key): ZIO[Any, Throwable, MessagePack] = for {
    body <- decodeToMap(packet.body)
    value <- ZIO.fromOption(body.get(key.value)).mapError(_ => IncorrectPacket(s"Packet has no $key value in body part", packet))
  } yield value

  private def extractErrorCode(code: Long): ZIO[Any, IncorrectCodeFormat, Long] =
    if ((code & Code.ErrorTypeMarker.value) == 0) {
      ZIO.fail(IncorrectCodeFormat(s"Code $code does not follow 0x8XXX format"))
    } else {
      ZIO.succeed(~Code.ErrorTypeMarker.value & code)
    }

  private def decodeToMap(value: MpMap): Task[Map[Long, MessagePack]] =
    ZIO.effect(mapEncoder.decodeUnsafe(value))
}

object PacketManagerLive {
  private val InitialRequestSize = 1024

  private val numberEncoder: Encoder[Long] = Encoder.longEncoder
  private val stringEncoder: Encoder[String] = Encoder.stringEncoder
  private val mapEncoder: Encoder[Map[Long, MessagePack]] = Encoder.mapEncoder[Long, MessagePack]

  final case class IncorrectPacket(reason: String, packet: MessagePackPacket)
      extends RuntimeException(s"Reason: $reason\nPacket: $packet")
      with NoStackTrace

  final case class IncorrectCodeFormat(reason: String) extends RuntimeException(reason) with NoStackTrace
}
