package zio.tarantool

import zio._
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.impl.TarantoolClientLive
import zio.tarantool.msgpack.MpArray
import zio.tarantool.protocol.IteratorCode

object TarantoolClient {
  type TarantoolClient = Has[Service]

  trait Service extends Serializable {
    def ping(): Task[TarantoolOperation]

    def select(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray): Task[TarantoolOperation]

    def insert(spaceId: Int, tuple: MpArray): Task[TarantoolOperation]

    def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[TarantoolOperation]

    def delete(spaceId: Int, indexId: Int, key: MpArray): Task[TarantoolOperation]

    def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[TarantoolOperation]

    def replace(spaceId: Int, tuple: MpArray): Task[TarantoolOperation]

    def call(functionName: String, args: MpArray): Task[TarantoolOperation]

    def eval(expression: String, args: MpArray): Task[TarantoolOperation]
  }

  def live(): ZLayer[TarantoolConnection, Nothing, TarantoolClient] =
    ZLayer.fromServiceManaged[TarantoolConnection.Service, Any, Nothing, Service](make)

  def make(connection: TarantoolConnection.Service): ZManaged[Any, Nothing, Service] =
    ZManaged.succeed(connection).map(new TarantoolClientLive(_))

  def ping(): RIO[TarantoolClient, TarantoolOperation] = ZIO.accessM(_.get.ping())

  def select(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): RIO[TarantoolClient, TarantoolOperation] = ZIO.accessM(_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def insert(spaceId: Int, tuple: MpArray): RIO[TarantoolClient, TarantoolOperation] = ZIO.accessM(_.get.insert(spaceId, tuple))

  def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.update(spaceId, indexId, key, tuple))

  def delete(spaceId: Int, indexId: Int, key: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.delete(spaceId, indexId, key))

  def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.upsert(spaceId, indexId, key, tuple))

  def replace(spaceId: Int, tuple: MpArray): RIO[TarantoolClient, TarantoolOperation] = ZIO.accessM(_.get.replace(spaceId, tuple))

  def call(functionName: String, args: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.call(functionName, args))

  def eval(expression: String, args: MpArray): RIO[TarantoolClient, TarantoolOperation] = ZIO.accessM(_.get.eval(expression, args))
}
