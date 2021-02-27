package zio.tarantool.protocol

import zio.IO
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.msgpack.{Encoder, MessagePack}

import scala.collection.mutable

final case class TarantoolRequest(
  operationCode: OperationCode,
  syncId: Long,
  schemaId: Long,
  body: Map[Long, MessagePack]
)

object TarantoolRequest {
  def createPacket(request: TarantoolRequest): IO[TarantoolError.CodecError, MessagePackPacket] = {
    val headerMp = mutable.Map[Long, MessagePack]()

    for {
      _ <- Encoder.longEncoder.encodeM(request.syncId).map(mp => headerMp += Key.Sync.value -> mp)
      _ <- Encoder.longEncoder
        .encodeM(request.operationCode.value)
        .map(mp => headerMp += Key.Code.value -> mp)
      _ <- Encoder.longEncoder
        .encodeM(request.schemaId)
        .map(mp => headerMp += Key.SchemaId.value -> mp)
    } yield MessagePackPacket(headerMp.toMap, request.body)
  }
}
