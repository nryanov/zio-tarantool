package zio.tarantool.api

import org.msgpack.value.Value
import zio._
import zio.tarantool.TarantoolError
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.internal.schema.SpaceMeta
import zio.tarantool.internal.{SyncIdProvider, TarantoolConnection}
import zio.tarantool.protocol._

object TarantoolSpaceOperations {
  type TarantoolSpaceOperations = Has[Service]

  trait Service {
    def select(key: Value, query: SelectQuery): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def select[A: TupleEncoder](
      key: A,
      query: SelectQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def insert(tuple: Value): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def insert[A: TupleEncoder](tuple: A): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def update(key: Value, updateQuery: UpdateQuery): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def update[A: TupleEncoder](
      key: A,
      updateQuery: UpdateQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def delete(key: Value): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def delete[A: TupleEncoder](key: A): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def upsert(
      tuple: Value,
      updateQuery: UpdateQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def upsert[A: TupleEncoder](
      tuple: A,
      updateQuery: UpdateQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def replace(tuple: Value): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def replace[A: TupleEncoder](tuple: A): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]
  }

  private case class Live(
    spaceMeta: SpaceMeta,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ) extends Service {
    override def select(
      key: Value,
      query: SelectQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      ???

    override def select[A: TupleEncoder](
      key: A,
      query: SelectQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def insert(tuple: Value): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def insert[A: TupleEncoder](tuple: A): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def update(
      key: Value,
      updateQuery: UpdateQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def update[A: TupleEncoder](
      key: A,
      updateQuery: UpdateQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def delete(key: Value): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def delete[A: TupleEncoder](key: A): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def upsert(
      tuple: Value,
      updateQuery: UpdateQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def upsert[A: TupleEncoder](
      tuple: A,
      updateQuery: UpdateQuery
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def replace(tuple: Value): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???

    override def replace[A: TupleEncoder](tuple: A): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      ???

    private def send(
      op: RequestCode,
      body: Map[Long, Value]
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        syncId <- syncIdProvider.syncId()
        request = TarantoolRequest(op, syncId, body)
        response <- connection.sendRequest(request).map(_.response)
      } yield response
  }
}
