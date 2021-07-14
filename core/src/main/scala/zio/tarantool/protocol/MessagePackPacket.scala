package zio.tarantool.protocol

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import org.msgpack.value.Value
import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.codec.{Encoder, MessagePackPacketCodec}

final case class MessagePackPacket(header: Map[Long, Value], body: Map[Long, Value])

object MessagePackPacket {
  private val InitialRequestSize = 1024

  def apply(
    header: Map[Long, Value],
    body: Option[Map[Long, Value]]
  ): MessagePackPacket = body match {
    case Some(value) => ValuePacket(header, value)
    case None        => ValuePacket(header)
  }

  def apply(header: Map[Long, Value], body: Map[Long, Value]): MessagePackPacket =
    new MessagePackPacket(header, body)

  def apply(header: Map[Long, Value]): MessagePackPacket =
    new MessagePackPacket(header, Map.empty)

  def toBuffer(packet: MessagePackPacket): IO[TarantoolError, ByteBuffer] = for {
    os <- ZIO.effectTotal(new ByteArrayOutputStream(InitialRequestSize))
    encodedPacket <- encodePacket(packet)
    size <- Encoder[Long].encodeM(encodedPacket.bytes.length)
    sizeMp <- encodeValue(size)
    _ <- ZIO.effect(os.write(sizeMp.toByteArray)).refineOrDie(toIOError)
    _ <- ZIO.effect(os.write(encodedPacket.toByteArray)).refineOrDie(toIOError)
  } yield ByteBuffer.wrap(os.toByteArray)

  def responseType(packet: MessagePackPacket): IO[TarantoolError, ResponseType] =
    packet.body match {
      case mp if mp.contains(ResponseBodyKey.Data.value)    => IO.succeed(ResponseType.DataResponse)
      case mp if mp.contains(ResponseBodyKey.SqlInfo.value) => IO.succeed(ResponseType.SqlResponse)
      case mp if mp.contains(ResponseBodyKey.Error24.value) =>
        IO.succeed(ResponseType.ErrorResponse)
      case mp if mp.contains(ResponseBodyKey.Error.value) => IO.succeed(ResponseType.ErrorResponse)
      case mp if mp.isEmpty                               => IO.succeed(ResponseType.PingResponse)
      case _                                              => IO.fail(TarantoolError.UnknownResponseCode(packet))
    }

  def extractCode(packet: MessagePackPacket): IO[TarantoolError, ResponseCode] = for {
    codeMp <- ZIO
      .fromOption(packet.header.get(Header.Code.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no Code in header (${packet.header})")
      )
    codeValue <- Encoder.intEncoder.decodeM(codeMp)
    code <- if (codeValue == 0) ZIO.succeed(ResponseCode.Success) else extractErrorCode(codeValue)
  } yield code

  def extractError(
    packet: MessagePackPacket
  ): IO[TarantoolError, String] =
    extractByKey(packet, ResponseBodyKey.Error24).flatMap(Encoder.stringEncoder.decodeM)

  def extractData(
    packet: MessagePackPacket
  ): IO[TarantoolError.ProtocolError, Value] =
    extractByKey(packet, ResponseBodyKey.Data)

  def extractSql(
    packet: MessagePackPacket
  ): IO[TarantoolError.ProtocolError, Value] =
    extractByKey(packet, ResponseBodyKey.SqlInfo)

  def extractSyncId(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    syncIdMp <- ZIO
      .fromOption(packet.header.get(Header.Sync.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SyncId in header (${packet.header})")
      )
    syncId <- Encoder.longEncoder.decodeM(syncIdMp)
  } yield syncId

  def extractSchemaId(packet: MessagePackPacket): IO[TarantoolError, Long] = for {
    schemaIdMp <- ZIO
      .fromOption(packet.header.get(Header.SchemaId.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SchemaId in header (${packet.header})")
      )
    schemaId <- Encoder.longEncoder.decodeM(schemaIdMp)
  } yield schemaId

  private def extractByKey(
    packet: MessagePackPacket,
    key: ResponseBodyKey
  ): ZIO[Any, TarantoolError.ProtocolError, Value] =
    for {
      value <- ZIO
        .fromOption(packet.body.get(key.value))
        .orElseFail(
          TarantoolError.ProtocolError(s"Packet has no $key value in body part ${packet.body}")
        )
    } yield value

  private def extractErrorCode(code: Int): ZIO[Any, TarantoolError.ProtocolError, ResponseCode] =
    if ((code & ResponseCode.errorTypeMarker) == 0) {
      ZIO.fail(TarantoolError.ProtocolError(s"Code $code does not follow 0x8XXX format"))
    } else {
      ZIO.succeed(ResponseCode.Error(~ResponseCode.errorTypeMarker & code))
    }

  private def encodePacket(packet: MessagePackPacket): IO[TarantoolError.CodecError, Array[Byte]] =
    ZIO.effect(MessagePackPacketCodec.encode(packet).require).mapError(TarantoolError.CodecError)

  private def encodeValue(mp: Value): IO[TarantoolError.CodecError, Array[Byte]] =
    IO.effect(ValueCodec.encode(mp).require).mapError(TarantoolError.CodecError)
}
