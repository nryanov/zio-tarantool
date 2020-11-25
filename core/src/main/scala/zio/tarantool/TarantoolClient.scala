package zio.tarantool

import zio._
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.impl.TarantoolClientLive
import zio.tarantool.msgpack.{MessagePack, MpArray}
import zio.tarantool.protocol.IteratorCode

object TarantoolClient {
  type TarantoolClient = Has[Service]

  trait Service {
    def ping(): Task[MessagePack]

    def select(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray): Task[MessagePack]

    def insert(spaceId: Int, tuple: MpArray): Task[MessagePack]

    def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePack]

    def delete(spaceId: Int, indexId: Int, key: MpArray): Task[MessagePack]

    def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePack]

    def replace(spaceId: Int, tuple: MpArray): Task[MessagePack]

    def call(functionName: String, args: MpArray): Task[MessagePack]

    def eval(expression: String, args: MpArray): Task[MessagePack]
  }

  val live: ZLayer[TarantoolConnection, Nothing, TarantoolClient] =
    ZLayer.fromService(new TarantoolClientLive(_))

  def ping(): RIO[TarantoolClient, MessagePack] = ZIO.accessM(_.get.ping())

  def select(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): RIO[TarantoolClient, MessagePack] = ZIO.accessM(_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def insert(spaceId: Int, tuple: MpArray): RIO[TarantoolClient, MessagePack] = ZIO.accessM(_.get.insert(spaceId, tuple))

  def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): RIO[TarantoolClient, MessagePack] =
    ZIO.accessM(_.get.update(spaceId, indexId, key, tuple))

  def delete(spaceId: Int, indexId: Int, key: MpArray): RIO[TarantoolClient, MessagePack] =
    ZIO.accessM(_.get.delete(spaceId, indexId, key))

  def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): RIO[TarantoolClient, MessagePack] =
    ZIO.accessM(_.get.upsert(spaceId, indexId, key, tuple))

  def replace(spaceId: Int, tuple: MpArray): RIO[TarantoolClient, MessagePack] = ZIO.accessM(_.get.replace(spaceId, tuple))

  def call(functionName: String, args: MpArray): RIO[TarantoolClient, MessagePack] =
    ZIO.accessM(_.get.call(functionName, args))

  def eval(expression: String, args: MpArray): RIO[TarantoolClient, MessagePack] = ZIO.accessM(_.get.eval(expression, args))
}
