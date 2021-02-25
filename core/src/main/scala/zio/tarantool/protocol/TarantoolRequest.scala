package zio.tarantool.protocol

import zio.{IO, ZIO}
import zio.tarantool.TarantoolError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.msgpack.{Encoder, MessagePack}

/**
 * @param operationCode - operation code
 * @param header - request header
 * @param body - request body
 */
final case class TarantoolRequest(
  operationCode: OperationCode,
  header: Map[Long, MessagePack],
  body: Map[Long, MessagePack]
) {
  val syncId: IO[TarantoolError, Long] = for {
    syncIdMp <- ZIO
      .fromOption(header.get(Key.Sync.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SyncId in header ($header)")
      )
    syncId <- Encoder.longEncoder.decodeM(syncIdMp)
  } yield syncId

  val schemaId: IO[TarantoolError, Long] = for {
    schemaIdMp <- ZIO
      .fromOption(header.get(Key.SchemaId.value))
      .orElseFail(
        TarantoolError.ProtocolError(s"Packet has no SchemaId in header ($header)")
      )
    schemaId <- Encoder.longEncoder.decodeM(schemaIdMp)
  } yield schemaId
}
