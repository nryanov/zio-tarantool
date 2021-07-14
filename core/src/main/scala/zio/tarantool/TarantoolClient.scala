package zio.tarantool

import zio._
import zio.clock.Clock
import zio.tarantool.internal._
import org.msgpack.core._
import org.msgpack.value.Value
import org.msgpack.value.impl.ImmutableArrayValueImpl
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.TarantoolRequestBody._
import zio.tarantool.protocol.{
  IteratorCode,
  RequestCode,
  TarantoolRequest,
  TarantoolResponse,
  UpdateOperations
}

object TarantoolClient {
  type TarantoolClient = Has[Service]

  private[this] val EmptyTuple = new ImmutableArrayValueImpl(Array.empty)

  trait Service extends Serializable {
    def ping(): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def refreshMeta(): IO[TarantoolError, Unit]

    def select(
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def select[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def select(
      spaceName: String,
      indexName: String,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def select[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def insert(
      spaceId: Int,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def insert[A: TupleEncoder](
      spaceId: Int,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def insert(
      spaceName: String,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def insert[A: TupleEncoder](
      spaceName: String,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def update(
      spaceId: Int,
      indexId: Int,
      key: Value,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def update[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A,
      updateOps: UpdateOperations
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def update(
      spaceName: String,
      indexName: String,
      key: Value,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def update[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      key: A,
      updateOps: UpdateOperations
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def delete(
      spaceId: Int,
      indexId: Int,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def delete[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def delete(
      spaceName: String,
      indexName: String,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def delete[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def upsert(
      spaceId: Int,
      indexId: Int,
      ops: Value,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def upsert[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      updateOps: UpdateOperations,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def upsert(
      spaceName: String,
      indexName: String,
      ops: Value,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def upsert[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      updateOps: UpdateOperations,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def replace(
      spaceId: Int,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def replace[A: TupleEncoder](
      spaceId: Int,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def replace(
      spaceName: String,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def replace[A: TupleEncoder](
      spaceName: String,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def call(
      functionName: String,
      args: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def call[A: TupleEncoder](
      functionName: String,
      args: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def call(functionName: String): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def eval(
      expression: String,
      args: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def eval[A: TupleEncoder](
      expression: String,
      args: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def eval(expression: String): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def execute(
      statementId: Int,
      sqlBind: Value,
      options: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    final def execute(
      statementId: Int,
      sqlBind: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      execute(statementId, sqlBind, EmptyTuple)

    final def execute(
      statementId: Int
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      execute(statementId, EmptyTuple)

    def execute(
      sql: String,
      sqlBind: Value,
      options: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    final def execute(
      sql: String,
      sqlBind: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      execute(sql, sqlBind, EmptyTuple)

    final def execute(sql: String): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      execute(sql, EmptyTuple)

    def prepare(statementId: Int): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def prepare(sql: String): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]
  }

  def ping(): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.ping())

  def refreshMeta(): ZIO[TarantoolClient, TarantoolError, Unit] =
    ZIO.accessM[TarantoolClient](_.get.refreshMeta())

  def select(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def select[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def select(
    spaceName: String,
    indexName: String,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceName, indexName, limit, offset, iterator, key))

  def select[A: TupleEncoder](
    spaceName: String,
    indexName: String,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceName, indexName, limit, offset, iterator, key))

  def insert(
    spaceId: Int,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceId, tuple))

  def insert[A: TupleEncoder](
    spaceId: Int,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceId, tuple))

  def insert(
    spaceName: String,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceName, tuple))

  def insert[A: TupleEncoder](
    spaceName: String,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceName, tuple))

  def update(
    spaceId: Int,
    indexId: Int,
    key: Value,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceId, indexId, key, tuple))

  def update[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A,
    updateOps: UpdateOperations
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceId, indexId, key, updateOps))

  def update(
    spaceName: String,
    indexName: String,
    key: Value,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceName, indexName, key, tuple))

  def update[A: TupleEncoder](
    spaceName: String,
    indexName: String,
    key: A,
    updateOps: UpdateOperations
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceName, indexName, key, updateOps))

  def delete(
    spaceId: Int,
    indexId: Int,
    key: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.delete(spaceId, indexId, key))

  def delete[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A
  ) = ZIO.accessM[TarantoolClient](_.get.delete(spaceId, indexId, key))

  def delete(
    spaceName: String,
    indexName: String,
    key: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.delete(spaceName, indexName, key))

  def delete[A: TupleEncoder](
    spaceName: String,
    indexName: String,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.delete(spaceName, indexName, key))

  def upsert(
    spaceId: Int,
    indexId: Int,
    ops: Value,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceId, indexId, ops, tuple))

  def upsert[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    updateOps: UpdateOperations,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceId, indexId, updateOps, tuple))

  def upsert(
    spaceName: String,
    indexName: String,
    ops: Value,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceName, indexName, ops, tuple))

  def upsert[A: TupleEncoder](
    spaceName: String,
    indexName: String,
    updateOps: UpdateOperations,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceName, indexName, updateOps, tuple))

  def replace(
    spaceId: Int,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceId, tuple))

  def replace[A: TupleEncoder](
    spaceId: Int,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceId, tuple))

  def replace(
    spaceName: String,
    tuple: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceName, tuple))

  def replace[A: TupleEncoder](
    spaceName: String,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceName, tuple))

  def call(
    functionName: String,
    args: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.call(functionName, args))

  def call[A: TupleEncoder](
    functionName: String,
    args: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.call(functionName, args))

  def call(
    functionName: String
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.call(functionName))

  def eval(
    expression: String,
    args: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.eval(expression, args))

  def eval[A: TupleEncoder](
    expression: String,
    args: A
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.eval(expression, args))

  def eval(
    expression: String
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.eval(expression))

  def execute(
    statementId: Int,
    sqlBind: Value,
    options: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.execute(statementId, sqlBind, options))

  def execute(
    statementId: Int,
    sqlBind: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.execute(statementId, sqlBind))

  def execute(
    statementId: Int
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.execute(statementId))

  def execute(
    sql: String,
    sqlBind: Value,
    options: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.execute(sql, sqlBind, options))

  def execute(
    sql: String,
    sqlBind: Value
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.execute(sql, sqlBind))

  def execute(
    sql: String
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.execute(sql))

  def prepare(
    statementId: Int
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.prepare(statementId))

  def prepare(
    sql: String
  ): ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.accessM[TarantoolClient](_.get.prepare(sql))

  val live: ZLayer[Has[TarantoolConfig] with Clock, TarantoolError, TarantoolClient] =
    ZLayer.fromServiceManaged[TarantoolConfig, Clock, TarantoolError, Service] { cfg =>
      make(cfg)
    }

  def make(config: TarantoolConfig): ZManaged[Clock, TarantoolError, Service] =
    for {
      syncIdProvider <- SyncIdProvider.make()
      requestHandler <- RequestHandler.make()
      connection <- TarantoolConnection.make(config, syncIdProvider, requestHandler)
      schemaMetaManager <- SchemaMetaManager.make(config, connection, syncIdProvider)
      _ <- ResponseHandler.make(connection, requestHandler)
      // fetch actual meta on start
      _ <- ZIO.when(config.clientConfig.useSchemaMetaCache)(schemaMetaManager.refresh).toManaged_
    } yield new Live(schemaMetaManager, connection, syncIdProvider)

  private[this] final class Live(
    schemaMetaManager: SchemaMetaManager.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ) extends TarantoolClient.Service {
    override def ping(): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      response <- send(RequestCode.Ping, Map.empty)
    } yield response

    override def refreshMeta(): IO[TarantoolError, Unit] = schemaMetaManager.refresh

    override def select(
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        body <- ZIO
          .effect(selectBody(spaceId, indexId, limit, offset, iterator, key))
          .mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Select, body)
      } yield response

    override def select[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      encodedKey <- TupleEncoder[A].encodeM(key)
      response <- select(spaceId, indexId, limit, offset, iterator, encodedKey)
    } yield response

    override def select(
      spaceName: String,
      indexName: String,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- select(meta.spaceId, meta.indexId, limit, offset, iterator, key)
    } yield response

    override def select[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- select(meta.spaceId, meta.indexId, limit, offset, iterator, key)
    } yield response

    override def insert(
      spaceId: Int,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        body <- ZIO.effect(insertBody(spaceId, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Insert, body)
      } yield response

    override def insert[A: TupleEncoder](
      spaceId: Int,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      encodedTuple <- ZIO.effect(TupleEncoder[A].encode(tuple)).mapError(TarantoolError.CodecError)
      response <- insert(spaceId, encodedTuple)
    } yield response

    override def insert(
      spaceName: String,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        meta <- schemaMetaManager.getSpaceMeta(spaceName)
        response <- insert(meta.spaceId, tuple)
      } yield response

    override def insert[A: TupleEncoder](
      spaceName: String,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        meta <- schemaMetaManager.getSpaceMeta(spaceName)
        response <- insert(meta.spaceId, tuple)
      } yield response

    override def update(
      spaceId: Int,
      indexId: Int,
      key: Value,
      ops: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      body <- ZIO.effect(updateBody(spaceId, indexId, key, ops)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Update, body)
    } yield response

    override def update[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A,
      updateOps: UpdateOperations
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???
    // fixme
//      for {
//      encodedKey <- TupleEncoder[A].encodeM(key)
//      encodedUpdateOps <- TupleEncoder[UpdateOperations].encodeM(updateOps)
//      response <- update(spaceId, indexId, encodedKey, encodedUpdateOps)
//    } yield response

    override def update(
      spaceName: String,
      indexName: String,
      key: Value,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- update(meta.spaceId, meta.indexId, key, tuple)
    } yield response

    override def update[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      key: A,
      updateOps: UpdateOperations
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- update(meta.spaceId, meta.indexId, key, updateOps)
    } yield response

    override def delete(
      spaceId: Int,
      indexId: Int,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      body <- ZIO.effect(deleteBody(spaceId, indexId, key)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Delete, body)
    } yield response

    override def delete[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      encodedKey <- TupleEncoder[A].encodeM(key)
      response <- delete(spaceId, indexId, encodedKey)
    } yield response

    override def delete(
      spaceName: String,
      indexName: String,
      key: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- delete(meta.spaceId, meta.indexId, key)
    } yield response

    override def delete[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      key: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- delete(meta.spaceId, meta.indexId, key)
    } yield response

    override def upsert(
      spaceId: Int,
      indexId: Int,
      ops: Value,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      body <- ZIO
        .effect(upsertBody(spaceId, indexId, ops, tuple))
        .mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Upsert, body)
    } yield response

    override def upsert[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      ops: UpdateOperations,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = ???
    // fixme
//      for {
//      encodedTuple <- TupleEncoder[A].encodeM(tuple)
//      encodedUpdateOps <- TupleEncoder[UpdateOperations].encodeM(ops)
//      response <- upsert(spaceId, indexId, encodedUpdateOps, encodedTuple)
//    } yield response

    override def upsert(
      spaceName: String,
      indexName: String,
      ops: Value,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- upsert(meta.spaceId, meta.indexId, ops, tuple)
    } yield response

    override def upsert[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      updateOps: UpdateOperations,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- upsert(meta.spaceId, meta.indexId, updateOps, tuple)
    } yield response

    override def replace(
      spaceId: Int,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        body <- ZIO.effect(replaceBody(spaceId, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Replace, body)
      } yield response

    override def replace[A: TupleEncoder](
      spaceId: Int,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      encodedTuple <- TupleEncoder[A].encodeM(tuple)
      response <- replace(spaceId, encodedTuple)
    } yield response

    override def replace(
      spaceName: String,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getSpaceMeta(spaceName)
      response <- replace(meta.spaceId, tuple)
    } yield response

    override def replace[A: TupleEncoder](
      spaceName: String,
      tuple: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      meta <- schemaMetaManager.getSpaceMeta(spaceName)
      response <- replace(meta.spaceId, tuple)
    } yield response

    override def call(
      functionName: String,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        body <- ZIO.effect(callBody(functionName, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Call, body)
      } yield response

    override def call(
      functionName: String
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      call(functionName, EmptyTuple)

    override def call[A: TupleEncoder](
      functionName: String,
      args: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        encodedArgs <- TupleEncoder[A].encodeM(args)
        response <- call(functionName, encodedArgs)
      } yield response

    override def eval(
      expression: String,
      tuple: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      for {
        body <- ZIO.effect(evalBody(expression, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Eval, body)
      } yield response

    override def eval(
      expression: String
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      eval(expression, EmptyTuple)

    override def eval[A: TupleEncoder](
      expression: String,
      args: A
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      encodedArgs <- TupleEncoder[A].encodeM(args)
      response <- eval(expression, encodedArgs)
    } yield response

    override def execute(
      statementId: Int,
      sqlBind: Value,
      options: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      body <- ZIO
        .effect(executeBody(statementId, sqlBind, options))
        .mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Execute, body)
    } yield response

    override def execute(
      sql: String,
      sqlBind: Value,
      options: Value
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      body <- ZIO.effect(executeBody(sql, sqlBind, options)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Execute, body)
    } yield response

    override def prepare(
      statementId: Int
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      body <- ZIO.effect(prepareBody(statementId)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Prepare, body)
    } yield response

    override def prepare(
      sql: String
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] = for {
      body <- ZIO.effect(prepareBody(sql)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Prepare, body)
    } yield response

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
