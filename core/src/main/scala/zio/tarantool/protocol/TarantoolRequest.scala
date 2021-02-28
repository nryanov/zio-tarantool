package zio.tarantool.protocol

import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.msgpack.{Encoder, MessagePack}

import scala.collection.mutable

final case class TarantoolRequest(
  operationCode: OperationCode,
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
        .map(mp => headerMp += FieldKey.Sync.value -> mp)
      _ <- Encoder.longEncoder
        .encodeM(request.operationCode.value)
        .map(mp => headerMp += FieldKey.Code.value -> mp)
      _ <- ZIO
        .fromOption(request.schemaId)
        .flatMap(schemaId =>
          Encoder.longEncoder.encodeM(schemaId).map(mp => headerMp += FieldKey.SchemaId.value -> mp)
        )
        .ignore
    } yield MessagePackPacket(headerMp.toMap, request.body)
  }
}
