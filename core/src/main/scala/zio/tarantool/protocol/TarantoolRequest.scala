package zio.tarantool.protocol

import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.{Encoder, MessagePack}
import zio.tarantool.protocol.Implicits._
import zio.{IO, ZIO}

import scala.collection.mutable

final case class TarantoolRequest(
  operationCode: RequestCode,
  syncId: Long,
  schemaId: Option[Long],
  body: Map[Long, MessagePack]
)

object TarantoolRequest {
  def createPacket(request: TarantoolRequest): IO[TarantoolError.CodecError, MessagePackPacket] = {
    val headerMp = mutable.Map[Long, MessagePack]()

    for {
      _ <- Encoder.longEncoder
        .encodeM(request.syncId)
        .map(mp => headerMp += Header.Sync.value -> mp)
      _ <- Encoder.longEncoder
        .encodeM(request.operationCode.value)
        .map(mp => headerMp += Header.Code.value -> mp)
      _ <- ZIO
        .fromOption(request.schemaId)
        .flatMap(schemaId =>
          Encoder.longEncoder.encodeM(schemaId).map(mp => headerMp += Header.SchemaId.value -> mp)
        )
        .ignore
    } yield MessagePackPacket(headerMp.toMap, request.body)
  }

  def withSchemaId(
    request: TarantoolRequest,
    schemaId: Long
  ): IO[TarantoolError.CodecError, TarantoolRequest] = for {
    schemaIdMp <- Encoder.longEncoder.encodeM(schemaId)
  } yield request.copy(
    schemaId = Some(schemaId),
    body = request.body.+(Header.SchemaId.value -> schemaIdMp)
  )
}
