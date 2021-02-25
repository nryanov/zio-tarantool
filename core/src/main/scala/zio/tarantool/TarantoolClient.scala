package zio.tarantool

import zio._
import zio.macros.accessible
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.impl.TarantoolClientLive
import zio.tarantool.msgpack.MpArray
import zio.tarantool.protocol.{IteratorCode, TarantoolOperation}

@accessible
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

  val live: ZLayer[TarantoolConnection, Nothing, TarantoolClient] =
    ZLayer.fromServiceManaged[TarantoolConnection.Service, Any, Nothing, Service](make)

  def make(connection: TarantoolConnection.Service): ZManaged[Any, Nothing, Service] =
    ZManaged.succeed(connection).map(new TarantoolClientLive(_))
}
