package zio.tarantool.impl

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import zio.ZIO
import zio.tarantool.PacketManager
import zio.tarantool.msgpack.Implicits._
import zio.tarantool.protocol.Implicits._
import zio.tarantool.msgpack.{Encoder, MessagePack}
import zio.tarantool.protocol.{Code, Key, MessagePackPacket, OperationCode}

import scala.collection.mutable
import PacketManagerLive._
import zio.tarantool.msgpack.MessagePackException.MessagePackEncodingException

import scala.util.control.NoStackTrace

final class PacketManagerLive extends PacketManager.Service {
  override def createPacket(
    op: OperationCode,
    syncId: Long,
    schemaId: Option[Long],
    body: Map[Long, MessagePack]
  ): ZIO[Any, Throwable, MessagePackPacket] = {
    val headerMp = mutable.Map[Long, MessagePack]()

    for {
      _ <- ZIO.effect(headerMp += Key.Sync.value -> numberEncoder.encodeUnsafe(syncId))
      _ <- ZIO.effect(headerMp += Key.Code.value -> numberEncoder.encodeUnsafe(op.value))
      _ <- ZIO.effect(schemaId.foreach(id => headerMp += Key.SchemaId.value -> numberEncoder.encodeUnsafe(id)))
    } yield MessagePackPacket(headerMp.toMap, body)
  }

  override def toBuffer(packet: MessagePackPacket): ZIO[Any, Throwable, ByteBuffer] = for {
    os <- ZIO.effectTotal(new ByteArrayOutputStream(InitialRequestSize))
    encodedPacket <- ZIO.effect(packet.encode().require)
    size <- ZIO.effect(numberEncoder.encodeUnsafe(encodedPacket.bytes.length))
    _ <- ZIO.effect(os.writeBytes(size.encode().require.toByteArray))
    _ <- ZIO.effect(os.writeBytes(encodedPacket.toByteArray))
  } yield ByteBuffer.wrap(os.toByteArray)

  override def extractCode(packet: MessagePackPacket): ZIO[Any, Throwable, Long] = for {
    codeMp <- ZIO.fromOption(packet.header.get(Key.Code.value)).mapError(_ => IncorrectPacket("Packet has no Code in header", packet))
    codeValue <- ZIO.effect(numberEncoder.decodeUnsafe(codeMp)).mapError(MessagePackEncodingException)
    code <- if (codeValue == 0) ZIO.succeed(codeValue) else extractErrorCode(codeValue)
  } yield code

  override def extractError(packet: MessagePackPacket): ZIO[Any, Throwable, String] =
    extractByKey(packet, Key.Error).map(stringEncoder.decodeUnsafe)

  override def extractData(packet: MessagePackPacket): ZIO[Any, Throwable, MessagePack] = extractByKey(packet, Key.Data)

  override def extractSyncId(packet: MessagePackPacket): ZIO[Any, Throwable, Long] = for {
    syncIdMp <- ZIO.fromOption(packet.header.get(Key.Sync.value)).mapError(_ => IncorrectPacket("Packet has no Code in header", packet))
    syncId <- ZIO.effect(numberEncoder.decodeUnsafe(syncIdMp)).mapError(MessagePackEncodingException)
  } yield syncId

  private def extractByKey(packet: MessagePackPacket, key: Key): ZIO[Any, Throwable, MessagePack] = for {
    value <- ZIO.fromOption(packet.body.get(key.value)).mapError(_ => IncorrectPacket(s"Packet has no $key value in body part", packet))
  } yield value

  private def extractErrorCode(code: Long): ZIO[Any, IncorrectCodeFormat, Long] =
    if ((code & Code.ErrorTypeMarker.value) == 0) {
      ZIO.fail(IncorrectCodeFormat(s"Code $code does not follow 0x8XXX format"))
    } else {
      ZIO.succeed(~Code.ErrorTypeMarker.value & code)
    }
}

object PacketManagerLive {
  private val InitialRequestSize = 1024

  private val numberEncoder: Encoder[Long] = Encoder.longEncoder
  private val stringEncoder: Encoder[String] = Encoder.stringEncoder

  final case class IncorrectPacket(reason: String, packet: MessagePackPacket)
      extends RuntimeException(s"Reason: $reason\nPacket: $packet")
      with NoStackTrace

  final case class IncorrectCodeFormat(reason: String) extends RuntimeException(reason) with NoStackTrace
}
