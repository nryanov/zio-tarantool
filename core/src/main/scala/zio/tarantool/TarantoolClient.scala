package zio.tarantool

import zio._
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.impl.TarantoolClientLive
import zio.tarantool.msgpack.MpArray
import zio.tarantool.protocol.{IteratorCode, TupleEncoder}

object TarantoolClient {
  type TarantoolClient = Has[Service]

  trait Service extends Serializable {
    def ping(): Task[TarantoolOperation]

    def select(
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: MpArray
    ): Task[TarantoolOperation]

    def select[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: A
    ): Task[TarantoolOperation]

    def insert(spaceId: Int, tuple: MpArray): Task[TarantoolOperation]

    def insert[A: TupleEncoder](spaceId: Int, tuple: A): Task[TarantoolOperation]

    def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[TarantoolOperation]

    def update[A: TupleEncoder, B: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A,
      tuple: B
    ): Task[TarantoolOperation]

    def delete(spaceId: Int, indexId: Int, key: MpArray): Task[TarantoolOperation]

    def delete[A: TupleEncoder](spaceId: Int, indexId: Int, key: A): Task[TarantoolOperation]

    def upsert(spaceId: Int, indexId: Int, ops: MpArray, tuple: MpArray): Task[TarantoolOperation]

    def upsert[A: TupleEncoder, B: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      ops: A,
      tuple: B
    ): Task[TarantoolOperation]

    def replace(spaceId: Int, tuple: MpArray): Task[TarantoolOperation]

    def replace[A: TupleEncoder](spaceId: Int, tuple: A): Task[TarantoolOperation]

    def call(functionName: String, args: MpArray): Task[TarantoolOperation]

    def call[A: TupleEncoder](functionName: String, args: A): Task[TarantoolOperation]

    def call(functionName: String): Task[TarantoolOperation]

    def eval(expression: String, args: MpArray): Task[TarantoolOperation]

    def eval[A: TupleEncoder](expression: String, args: A): Task[TarantoolOperation]

    def eval(expression: String): Task[TarantoolOperation]
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
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def select[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: A
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def insert(spaceId: Int, tuple: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.insert(spaceId, tuple))

  def insert[A: TupleEncoder](spaceId: Int, tuple: A): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.insert(spaceId, tuple))

  def update(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    tuple: MpArray
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.update(spaceId, indexId, key, tuple))

  def update[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A,
    tuple: B
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.update(spaceId, indexId, key, tuple))

  def delete(spaceId: Int, indexId: Int, key: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.delete(spaceId, indexId, key))

  def delete[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.delete(spaceId, indexId, key))

  def upsert(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    tuple: MpArray
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.upsert(spaceId, indexId, key, tuple))

  def upsert[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A,
    tuple: B
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.upsert(spaceId, indexId, key, tuple))

  def replace(spaceId: Int, tuple: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.replace(spaceId, tuple))

  def replace[A: TupleEncoder](spaceId: Int, tuple: A): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.replace(spaceId, tuple))

  def call(functionName: String, args: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.call(functionName, args))

  def call[A: TupleEncoder](
    functionName: String,
    args: A
  ): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.call(functionName, args))

  def call(functionName: String): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.call(functionName))

  def eval(expression: String, args: MpArray): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.eval(expression, args))

  def eval[A: TupleEncoder](expression: String, args: A): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.eval(expression, args))

  def eval(expression: String): RIO[TarantoolClient, TarantoolOperation] =
    ZIO.accessM(_.get.eval(expression))
}
