package zio.tarantool

import zio._
import zio.logging._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.core._
import zio.tarantool.msgpack._
import zio.tarantool.msgpack.MpArray
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.TarantoolRequestBody._
import zio.tarantool.protocol.{IteratorCode, RequestCode, TarantoolOperation, TarantoolRequest}

@accessible[TarantoolClient.Service]
object TarantoolClient {
  type TarantoolClient = Has[Service]

  private[this] val EmptyTuple = MpFixArray(Vector.empty)

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

  val live: ZLayer[Has[TarantoolConfig] with Logging with Clock, TarantoolError, TarantoolClient] =
    ZLayer.fromServiceManaged[TarantoolConfig, Logging with Clock, TarantoolError, Service] { cfg =>
      make(cfg)
    }

  def make(config: TarantoolConfig): ZManaged[Clock with Logging, TarantoolError, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      syncIdProvider <- SyncIdProvider.make()
      connection <- TarantoolConnection.make(config, syncIdProvider)
      requestHandler <- RequestHandler.make()
      schemaMetaManager <- SchemaMetaManager.make(
        config,
        requestHandler,
        connection,
        syncIdProvider
      )
      // todo: unused ???
      responseHandler <- ResponseHandler.make(connection, schemaMetaManager, requestHandler)
      // fetch actual meta on start
      _ <- schemaMetaManager.refresh.toManaged_
    } yield new Live(logger, schemaMetaManager, requestHandler, connection, syncIdProvider)

  private[this] final class Live(
    logger: Logger[String],
    schemaMetaManager: SchemaMetaManager.Service,
    requestHandler: RequestHandler.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ) extends TarantoolClient.Service {
    override def ping(): IO[TarantoolError, TarantoolOperation] = for {
      response <- send(RequestCode.Ping, Map.empty)
    } yield response

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
        schemaId <- op match {
          // do not refresh schema meta cache on `eval` and `call` requests
          case RequestCode.Eval | RequestCode.Call => ZIO.none
          case _                                   => schemaMetaManager.schemaId
        }
        syncId <- syncIdProvider.syncId()
        request = TarantoolRequest(op, syncId, schemaId, body)
        _ <- logger.debug(s"Submit operation: $syncId")
        operation <- requestHandler.submitRequest(request)
        packet <- TarantoolRequest.createPacket(request)
        _ <- connection.sendRequest(packet)
      } yield operation
  }
}
