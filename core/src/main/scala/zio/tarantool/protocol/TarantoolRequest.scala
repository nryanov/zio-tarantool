package zio.tarantool.protocol

import zio._
import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.{Encoder, MessagePack}
import zio.tarantool.protocol.Implicits._

import scala.collection.mutable

final case class TarantoolRequest(
  operationCode: RequestCode,
  syncId: Long,
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
    } yield MessagePackPacket(headerMp.toMap, request.body)
  }
}
