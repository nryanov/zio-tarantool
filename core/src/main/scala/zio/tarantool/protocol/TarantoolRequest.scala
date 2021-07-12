package zio.tarantool.protocol

import zio._
import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.{Encoder, MessagePack}
import zio.tarantool.protocol.Implicits._

final case class TarantoolRequest(
  operationCode: RequestCode,
  syncId: Long,
  body: Map[Long, MessagePack]
)

object TarantoolRequest {
  def createPacket(request: TarantoolRequest): IO[TarantoolError.CodecError, MessagePackPacket] =
    for {
      sync <- Encoder.longEncoder.encodeM(request.syncId)
      code <- Encoder.intEncoder.encodeM(request.operationCode.value)
    } yield MessagePackPacket(
      Map(
        Header.Sync.value -> sync,
        Header.Code.value -> code
      ),
      request.body
    )
}
