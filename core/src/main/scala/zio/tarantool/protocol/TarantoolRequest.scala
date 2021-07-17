package zio.tarantool.protocol

import org.msgpack.value.Value
import zio._
import zio.tarantool.TarantoolError
import zio.tarantool.codec.Encoder
import zio.tarantool.protocol.Implicits._

final case class TarantoolRequest(
  operationCode: RequestCode,
  syncId: Long,
  body: Map[Long, Value]
)

object TarantoolRequest {
  def createPacket(request: TarantoolRequest): IO[TarantoolError.CodecError, MessagePackPacket] =
    for {
      sync <- Encoder[Long].encodeM(request.syncId)
      code <- Encoder[Int].encodeM(request.operationCode.value)
    } yield MessagePackPacket(
      Map(
        Header.Sync.value -> sync,
        Header.Code.value -> code
      ),
      request.body
    )
}
