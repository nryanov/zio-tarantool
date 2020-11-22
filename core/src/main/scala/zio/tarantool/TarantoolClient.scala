package zio.tarantool

import zio._
import zio.macros.accessible
import zio.tarantool.msgpack.MpArray
import zio.tarantool.protocol.{IteratorCode, MessagePackPacket}

@accessible
object TarantoolClient {
  type TarantoolClientService = Has[Service]

  trait Service {
    def ping(): Task[MessagePackPacket]

    def select(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray): Task[MessagePackPacket]

    def insert(spaceId: Int, tuple: MpArray): Task[MessagePackPacket]

    def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePackPacket]

    def delete(spaceId: Int, indexId: Int, key: MpArray): Task[MessagePackPacket]

    def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePackPacket]

    def replace(spaceId: Int, tuple: MpArray): Task[MessagePackPacket]

    def call(functionName: String, tuple: MpArray): Task[MessagePackPacket]

    def eval(expression: String, args: MpArray): Task[MessagePackPacket]
  }
}
