package zio.tarantool

import zio._
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.impl.TarantoolClientLive
import zio.tarantool.msgpack.MpArray
import zio.tarantool.protocol.{IteratorCode, TupleEncoder}

object TarantoolClient {
  type TarantoolClient = Has[Service]

  trait Service extends Serializable {
    def ping(): IO[TarantoolError, TarantoolOperation]

    def select(
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    def select[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: A
    ): IO[TarantoolError, TarantoolOperation]

    def insert(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation]

    def insert[A: TupleEncoder](spaceId: Int, tuple: A): IO[TarantoolError, TarantoolOperation]

    def update(
      spaceId: Int,
      indexId: Int,
      key: MpArray,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    def update[A: TupleEncoder, B: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation]

    def delete(spaceId: Int, indexId: Int, key: MpArray): IO[TarantoolError, TarantoolOperation]

    def delete[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A
    ): IO[TarantoolError, TarantoolOperation]

    def upsert(
      spaceId: Int,
      indexId: Int,
      ops: MpArray,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    def upsert[A: TupleEncoder, B: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      ops: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation]

    def replace(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation]

    def replace[A: TupleEncoder](spaceId: Int, tuple: A): IO[TarantoolError, TarantoolOperation]

    def call(functionName: String, args: MpArray): IO[TarantoolError, TarantoolOperation]

    def call[A: TupleEncoder](functionName: String, args: A): IO[TarantoolError, TarantoolOperation]

    def call(functionName: String): IO[TarantoolError, TarantoolOperation]

    def eval(expression: String, args: MpArray): IO[TarantoolError, TarantoolOperation]

    def eval[A: TupleEncoder](expression: String, args: A): IO[TarantoolError, TarantoolOperation]

    def eval(expression: String): IO[TarantoolError, TarantoolOperation]
  }

  def live(): ZLayer[TarantoolConnection, Nothing, TarantoolClient] =
    ZLayer.fromServiceManaged[TarantoolConnection.Service, Any, Nothing, Service](make)

  def make(connection: TarantoolConnection.Service): ZManaged[Any, Nothing, Service] =
    ZManaged.succeed(connection).map(new TarantoolClientLive(_))

  def ping(): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] = ZIO.accessM(_.get.ping())

  def select(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def select[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def insert(
    spaceId: Int,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.insert(spaceId, tuple))

  def insert[A: TupleEncoder](
    spaceId: Int,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.insert(spaceId, tuple))

  def update(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    ops: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.update(spaceId, indexId, key, ops))

  def update[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A,
    ops: B
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.update(spaceId, indexId, key, ops))

  def delete(
    spaceId: Int,
    indexId: Int,
    key: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.delete(spaceId, indexId, key))

  def delete[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.delete(spaceId, indexId, key))

  def upsert(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.upsert(spaceId, indexId, key, tuple))

  def upsert[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A,
    tuple: B
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.upsert(spaceId, indexId, key, tuple))

  def replace(
    spaceId: Int,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.replace(spaceId, tuple))

  def replace[A: TupleEncoder](
    spaceId: Int,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.replace(spaceId, tuple))

  def call(
    functionName: String,
    args: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.call(functionName, args))

  def call[A: TupleEncoder](
    functionName: String,
    args: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.call(functionName, args))

  def call(functionName: String): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.call(functionName))

  def eval(
    expression: String,
    args: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.eval(expression, args))

  def eval[A: TupleEncoder](
    expression: String,
    args: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.eval(expression, args))

  def eval(expression: String): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM(_.get.eval(expression))
}
