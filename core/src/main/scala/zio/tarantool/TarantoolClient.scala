package zio.tarantool

import zio._
import zio.logging._
import zio.clock.Clock
import zio.tarantool.internal._
import zio.tarantool.msgpack._
import zio.tarantool.msgpack.MpArray
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.TarantoolRequestBody._
import zio.tarantool.protocol.{IteratorCode, RequestCode, TarantoolOperation, TarantoolRequest}

object TarantoolClient {
  type TarantoolClient = Has[Service]

  private[this] val EmptyTuple = MpFixArray(Vector.empty)

  trait Service extends Serializable {
    def ping(): IO[TarantoolError, TarantoolOperation]

    def refreshMeta(): IO[TarantoolError, Unit]

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

    def select(
      spaceName: String,
      indexName: String,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    def select[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: A
    ): IO[TarantoolError, TarantoolOperation]

    def insert(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation]

    def insert[A: TupleEncoder](spaceId: Int, tuple: A): IO[TarantoolError, TarantoolOperation]

    def insert(spaceName: String, tuple: MpArray): IO[TarantoolError, TarantoolOperation]

    def insert[A: TupleEncoder](spaceName: String, tuple: A): IO[TarantoolError, TarantoolOperation]

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

    def update(
      spaceName: String,
      indexName: String,
      key: MpArray,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    def update[A: TupleEncoder, B: TupleEncoder](
      spaceName: String,
      indexName: String,
      key: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation]

    def delete(spaceId: Int, indexId: Int, key: MpArray): IO[TarantoolError, TarantoolOperation]

    def delete[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A
    ): IO[TarantoolError, TarantoolOperation]

    def delete(
      spaceName: String,
      indexName: String,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    def delete[A: TupleEncoder](
      spaceName: String,
      indexName: String,
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

    def upsert(
      spaceName: String,
      indexName: String,
      ops: MpArray,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    def upsert[A: TupleEncoder, B: TupleEncoder](
      spaceName: String,
      indexName: String,
      ops: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation]

    def replace(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation]

    def replace[A: TupleEncoder](spaceId: Int, tuple: A): IO[TarantoolError, TarantoolOperation]

    def replace(spaceName: String, tuple: MpArray): IO[TarantoolError, TarantoolOperation]

    def replace[A: TupleEncoder](
      spaceName: String,
      tuple: A
    ): IO[TarantoolError, TarantoolOperation]

    def call(functionName: String, args: MpArray): IO[TarantoolError, TarantoolOperation]

    def call[A: TupleEncoder](functionName: String, args: A): IO[TarantoolError, TarantoolOperation]

    def call(functionName: String): IO[TarantoolError, TarantoolOperation]

    def eval(expression: String, args: MpArray): IO[TarantoolError, TarantoolOperation]

    def eval[A: TupleEncoder](expression: String, args: A): IO[TarantoolError, TarantoolOperation]

    def eval(expression: String): IO[TarantoolError, TarantoolOperation]

    def execute(
      statementId: Int,
      sqlBind: MpArray,
      options: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    final def execute(statementId: Int, sqlBind: MpArray): IO[TarantoolError, TarantoolOperation] =
      execute(statementId, sqlBind, EmptyTuple)

    final def execute(statementId: Int): IO[TarantoolError, TarantoolOperation] =
      execute(statementId, EmptyTuple)

    def execute(
      sql: String,
      sqlBind: MpArray,
      options: MpArray
    ): IO[TarantoolError, TarantoolOperation]

    final def execute(sql: String, sqlBind: MpArray): IO[TarantoolError, TarantoolOperation] =
      execute(sql, sqlBind, EmptyTuple)

    final def execute(sql: String): IO[TarantoolError, TarantoolOperation] =
      execute(sql, EmptyTuple)

    def prepare(statementId: Int): IO[TarantoolError, TarantoolOperation]

    def prepare(sql: String): IO[TarantoolError, TarantoolOperation]
  }

  def ping(): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.ping())

  def refreshMeta(): ZIO[TarantoolClient, TarantoolError, Unit] =
    ZIO.accessM[TarantoolClient](_.get.refreshMeta())

  def select(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def select[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceId, indexId, limit, offset, iterator, key))

  def select(
    spaceName: String,
    indexName: String,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceName, indexName, limit, offset, iterator, key))

  def select[A: TupleEncoder](
    spaceName: String,
    indexName: String,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.select(spaceName, indexName, limit, offset, iterator, key))

  def insert(
    spaceId: Int,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceId, tuple))

  def insert[A: TupleEncoder](
    spaceId: Int,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceId, tuple))

  def insert(
    spaceName: String,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceName, tuple))

  def insert[A: TupleEncoder](
    spaceName: String,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.insert(spaceName, tuple))

  def update(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceId, indexId, key, tuple))

  def update[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A,
    tuple: B
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceId, indexId, key, tuple))

  def update(
    spaceName: String,
    indexName: String,
    key: MpArray,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceName, indexName, key, tuple))

  def update[A: TupleEncoder, B: TupleEncoder](
    spaceName: String,
    indexName: String,
    key: A,
    tuple: B
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.update(spaceName, indexName, key, tuple))

  def delete(
    spaceId: Int,
    indexId: Int,
    key: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.delete(spaceId, indexId, key))

  def delete[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A
  ) = ZIO.accessM[TarantoolClient](_.get.delete(spaceId, indexId, key))

  def delete(
    spaceName: String,
    indexName: String,
    key: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.delete(spaceName, indexName, key))

  def delete[A: TupleEncoder](
    spaceName: String,
    indexName: String,
    key: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.delete(spaceName, indexName, key))

  def upsert(
    spaceId: Int,
    indexId: Int,
    ops: MpArray,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceId, indexId, ops, tuple))

  def upsert[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    ops: A,
    tuple: B
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceId, indexId, ops, tuple))

  def upsert(
    spaceName: String,
    indexName: String,
    ops: MpArray,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceName, indexName, ops, tuple))

  def upsert[A: TupleEncoder, B: TupleEncoder](
    spaceName: String,
    indexName: String,
    ops: A,
    tuple: B
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.upsert(spaceName, indexName, ops, tuple))

  def replace(
    spaceId: Int,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceId, tuple))

  def replace[A: TupleEncoder](
    spaceId: Int,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceId, tuple))

  def replace(
    spaceName: String,
    tuple: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceName, tuple))

  def replace[A: TupleEncoder](
    spaceName: String,
    tuple: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.replace(spaceName, tuple))

  def call(
    functionName: String,
    args: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.call(functionName, args))

  def call[A: TupleEncoder](
    functionName: String,
    args: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.call(functionName, args))

  def call(functionName: String): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.call(functionName))

  def eval(
    expression: String,
    args: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.eval(expression, args))

  def eval[A: TupleEncoder](
    expression: String,
    args: A
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.eval(expression, args))

  def eval(expression: String): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.eval(expression))

  def execute(
    statementId: Int,
    sqlBind: MpArray,
    options: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.execute(statementId, sqlBind, options))

  def execute(
    statementId: Int,
    sqlBind: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.execute(statementId, sqlBind))

  def execute(statementId: Int): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.execute(statementId))

  def execute(
    sql: String,
    sqlBind: MpArray,
    options: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.execute(sql, sqlBind, options))

  def execute(
    sql: String,
    sqlBind: MpArray
  ): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.execute(sql, sqlBind))

  def execute(sql: String): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.execute(sql))

  def prepare(statementId: Int): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.prepare(statementId))

  def prepare(sql: String): ZIO[TarantoolClient, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolClient](_.get.prepare(sql))

  val live: ZLayer[Has[TarantoolConfig] with Logging with Clock, TarantoolError, TarantoolClient] =
    ZLayer.fromServiceManaged[TarantoolConfig, Logging with Clock, TarantoolError, Service] { cfg =>
      make(cfg)
    }

  def make(config: TarantoolConfig): ZManaged[Clock with Logging, TarantoolError, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      syncIdProvider <- SyncIdProvider.make()
      requestHandler <- RequestHandler.make()
      connection <- TarantoolConnection.make(config, syncIdProvider, requestHandler)
      schemaMetaManager <- SchemaMetaManager.make(config, connection, syncIdProvider)
      _ <- ResponseHandler.make(connection, requestHandler)
      // fetch actual meta on start
      _ <- schemaMetaManager.refresh.toManaged_
    } yield new Live(logger, schemaMetaManager, connection, syncIdProvider)

  private[this] final class Live(
    logger: Logger[String],
    schemaMetaManager: SchemaMetaManager.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ) extends TarantoolClient.Service {
    override def ping(): IO[TarantoolError, TarantoolOperation] = for {
      response <- send(RequestCode.Ping, Map.empty)
    } yield response

    override def refreshMeta(): IO[TarantoolError, Unit] = schemaMetaManager.refresh

    override def select(
      spaceId: Int,
      indexId: Int,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation] =
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
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedKey <- TupleEncoder[A].encodeM(key)
      response <- select(spaceId, indexId, limit, offset, iterator, encodedKey)
    } yield response

    override def select(
      spaceName: String,
      indexName: String,
      limit: Int,
      offset: Int,
      iterator: IteratorCode,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
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
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- select(meta.spaceId, meta.indexId, limit, offset, iterator, key)
    } yield response

    override def insert(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation] =
      for {
        body <- ZIO.effect(insertBody(spaceId, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Insert, body)
      } yield response

    override def insert[A: TupleEncoder](
      spaceId: Int,
      tuple: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedTuple <- ZIO
        .effect(TupleEncoder[A].encodeUnsafe(tuple))
        .mapError(TarantoolError.CodecError)
      response <- insert(spaceId, encodedTuple)
    } yield response

    override def insert(spaceName: String, tuple: MpArray): IO[TarantoolError, TarantoolOperation] =
      for {
        meta <- schemaMetaManager.getSpaceMeta(spaceName)
        response <- insert(meta.spaceId, tuple)
      } yield response

    override def insert[A: TupleEncoder](
      spaceName: String,
      tuple: A
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        meta <- schemaMetaManager.getSpaceMeta(spaceName)
        response <- insert(meta.spaceId, tuple)
      } yield response

    override def update(
      spaceId: Int,
      indexId: Int,
      key: MpArray,
      ops: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO.effect(updateBody(spaceId, indexId, key, ops)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Update, body)
    } yield response

    override def update[A: TupleEncoder, B: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedKey <- TupleEncoder[A].encodeM(key)
      encodedTuple <- TupleEncoder[B].encodeM(tuple)
      response <- update(spaceId, indexId, encodedKey, encodedTuple)
    } yield response

    override def update(
      spaceName: String,
      indexName: String,
      key: MpArray,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- update(meta.spaceId, meta.indexId, key, tuple)
    } yield response

    override def update[A: TupleEncoder, B: TupleEncoder](
      spaceName: String,
      indexName: String,
      key: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- update(meta.spaceId, meta.indexId, key, tuple)
    } yield response

    override def delete(
      spaceId: Int,
      indexId: Int,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO.effect(deleteBody(spaceId, indexId, key)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Delete, body)
    } yield response

    override def delete[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedKey <- TupleEncoder[A].encodeM(key)
      response <- delete(spaceId, indexId, encodedKey)
    } yield response

    override def delete(
      spaceName: String,
      indexName: String,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- delete(meta.spaceId, meta.indexId, key)
    } yield response

    override def delete[A: TupleEncoder](
      spaceName: String,
      indexName: String,
      key: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- delete(meta.spaceId, meta.indexId, key)
    } yield response

    override def upsert(
      spaceId: Int,
      indexId: Int,
      ops: MpArray,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO
        .effect(upsertBody(spaceId, indexId, ops, tuple))
        .mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Upsert, body)
    } yield response

    override def upsert[A: TupleEncoder, B: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      ops: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedKey <- TupleEncoder[A].encodeM(ops)
      encodedTuple <- TupleEncoder[B].encodeM(tuple)
      response <- upsert(spaceId, indexId, encodedKey, encodedTuple)
    } yield response

    override def upsert(
      spaceName: String,
      indexName: String,
      ops: MpArray,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- upsert(meta.spaceId, meta.indexId, ops, tuple)
    } yield response

    override def upsert[A: TupleEncoder, B: TupleEncoder](
      spaceName: String,
      indexName: String,
      ops: A,
      tuple: B
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getIndexMeta(spaceName, indexName)
      response <- upsert(meta.spaceId, meta.indexId, ops, tuple)
    } yield response

    override def replace(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation] =
      for {
        body <- ZIO.effect(replaceBody(spaceId, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Replace, body)
      } yield response

    override def replace[A: TupleEncoder](
      spaceId: Int,
      tuple: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedTuple <- TupleEncoder[A].encodeM(tuple)
      response <- replace(spaceId, encodedTuple)
    } yield response

    override def replace(
      spaceName: String,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getSpaceMeta(spaceName)
      response <- replace(meta.spaceId, tuple)
    } yield response

    override def replace[A: TupleEncoder](
      spaceName: String,
      tuple: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      meta <- schemaMetaManager.getSpaceMeta(spaceName)
      response <- replace(meta.spaceId, tuple)
    } yield response

    override def call(
      functionName: String,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        body <- ZIO.effect(callBody(functionName, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Call, body)
      } yield response

    override def call(functionName: String): IO[TarantoolError, TarantoolOperation] =
      call(functionName, EmptyTuple)

    override def call[A: TupleEncoder](
      functionName: String,
      args: A
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        encodedArgs <- TupleEncoder[A].encodeM(args)
        response <- call(functionName, encodedArgs)
      } yield response

    override def eval(expression: String, tuple: MpArray): IO[TarantoolError, TarantoolOperation] =
      for {
        body <- ZIO.effect(evalBody(expression, tuple)).mapError(TarantoolError.CodecError)
        response <- send(RequestCode.Eval, body)
      } yield response

    override def eval(expression: String): IO[TarantoolError, TarantoolOperation] =
      eval(expression, EmptyTuple)

    override def eval[A: TupleEncoder](
      expression: String,
      args: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedArgs <- TupleEncoder[A].encodeM(args)
      response <- eval(expression, encodedArgs)
    } yield response

    override def execute(
      statementId: Int,
      sqlBind: MpArray,
      options: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO
        .effect(executeBody(statementId, sqlBind, options))
        .mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Execute, body)
    } yield response

    override def execute(
      sql: String,
      sqlBind: MpArray,
      options: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO.effect(executeBody(sql, sqlBind, options)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Execute, body)
    } yield response

    override def prepare(statementId: Int): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO.effect(prepareBody(statementId)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Prepare, body)
    } yield response

    override def prepare(sql: String): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO.effect(prepareBody(sql)).mapError(TarantoolError.CodecError)
      response <- send(RequestCode.Prepare, body)
    } yield response

    private def send(
      op: RequestCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        syncId <- syncIdProvider.syncId()
        request = TarantoolRequest(op, syncId, body)
        _ <- logger.debug(s"Submit operation: $syncId")
        operation <- connection.sendRequest(request)
      } yield operation
  }
}
