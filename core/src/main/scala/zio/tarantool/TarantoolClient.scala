package zio.tarantool

import _root_.zio._
import zio.tarantool.api._
import zio.tarantool.internal._
import org.msgpack.value.Value
import zio.tarantool.protocol.TarantoolRequestBody._
import zio.tarantool.protocol.{RequestCode, TarantoolRequest, TarantoolResponse}

object TarantoolClient {
  type TarantoolClient = Service

  trait Service extends Serializable {
    def ping(): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]

    def refreshMeta(): IO[TarantoolError, Unit]

    private[tarantool] def execute(
      request: BuiltRequest
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]]
  }

  def ping(): ZIO[Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    ZIO.serviceWithZIO(_.ping())

  def refreshMeta(): ZIO[Service, TarantoolError, Unit] =
    ZIO.serviceWithZIO(_.refreshMeta())

  def select: SelectBuilder = SelectBuilder()

  def insert: InsertBuilder = InsertBuilder()

  def replace: ReplaceBuilder = ReplaceBuilder()

  def delete: DeleteBuilder = DeleteBuilder()

  def update: UpdateBuilder = UpdateBuilder()

  def upsert: UpsertBuilder = UpsertBuilder()

  def call: CallBuilder = CallBuilder()

  def eval: EvalBuilder = EvalBuilder()

  def execute: ExecuteBuilder = ExecuteBuilder()

  def prepare: PrepareBuilder = PrepareBuilder()

  val live: ZLayer[TarantoolConfig with Clock, TarantoolError, Service] =
    ZLayer.scoped {
      ZIO.serviceWithZIO[TarantoolConfig](make)
    }

  def make(config: TarantoolConfig): ZIO[Scope with Clock, TarantoolError, Service] =
    for {
      syncIdProvider <- SyncIdProvider.make()
      requestHandler <- RequestHandler.make()
      connection <- TarantoolConnection.make(config, syncIdProvider, requestHandler)
      schemaMetaManager <- SchemaMetaManager.make(config, connection, syncIdProvider)
      _ <- ResponseHandler.make(connection, requestHandler)
      // fetch actual meta on start
      _ <- ZIO.when(config.clientConfig.useSchemaMetaCache)(schemaMetaManager.refresh)
    } yield new Live(schemaMetaManager, connection, syncIdProvider)

  private[this] final class Live(
    schemaMetaManager: SchemaMetaManager.Service,
    connection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ) extends TarantoolClient.Service {
    override def ping(): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      send(RequestCode.Ping, Map.empty)

    override def refreshMeta(): IO[TarantoolError, Unit] = schemaMetaManager.refresh

    override def execute(
      request: BuiltRequest
    ): IO[TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
      request match {
        case BuiltRequest.Select(space, index, limit, offset, iterator, key) =>
          for {
            ids <- resolveIndex(space, index)
            encodedKey <- key.encode
            body <- ZIO
              .attempt(selectBody(ids._1, ids._2, limit, offset, iterator, encodedKey))
              .mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Select, body)
          } yield response

        case BuiltRequest.Insert(space, tuple) =>
          for {
            spaceId <- resolveSpace(space)
            encodedTuple <- tuple.encode
            body <- ZIO.attempt(insertBody(spaceId, encodedTuple)).mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Insert, body)
          } yield response

        case BuiltRequest.Replace(space, tuple) =>
          for {
            spaceId <- resolveSpace(space)
            encodedTuple <- tuple.encode
            body <- ZIO.attempt(replaceBody(spaceId, encodedTuple)).mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Replace, body)
          } yield response

        case BuiltRequest.Delete(space, index, key) =>
          for {
            ids <- resolveIndex(space, index)
            encodedKey <- key.encode
            body <- ZIO.attempt(deleteBody(ids._1, ids._2, encodedKey)).mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Delete, body)
          } yield response

        case BuiltRequest.Update(space, index, key, ops) =>
          for {
            ids <- resolveIndex(space, index)
            encodedKey <- key.encode
            encodedOps <- ops.encode
            body <- ZIO.attempt(updateBody(ids._1, ids._2, encodedKey, encodedOps)).mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Update, body)
          } yield response

        case BuiltRequest.Upsert(space, index, ops, tuple) =>
          for {
            ids <- resolveIndex(space, index)
            encodedOps <- ops.encode
            encodedTuple <- tuple.encode
            body <- ZIO
              .attempt(upsertBody(ids._1, ids._2, encodedOps, encodedTuple))
              .mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Upsert, body)
          } yield response

        case BuiltRequest.Call(functionName, args) =>
          for {
            encodedArgs <- args.encode
            body <- ZIO.attempt(callBody(functionName, encodedArgs)).mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Call, body)
          } yield response

        case BuiltRequest.Eval(expression, args) =>
          for {
            encodedArgs <- args.encode
            body <- ZIO.attempt(evalBody(expression, encodedArgs)).mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Eval, body)
          } yield response

        case BuiltRequest.Execute(target, sqlBind, options) =>
          for {
            encodedBind <- sqlBind.encode
            encodedOptions <- options.encode
            body <- ZIO.attempt {
              target match {
                case BuiltRequest.ExecuteTarget.StatementId(id) =>
                  executeBody(id, encodedBind, encodedOptions)
                case BuiltRequest.ExecuteTarget.Sql(sql) =>
                  executeBody(sql, encodedBind, encodedOptions)
              }
            }.mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Execute, body)
          } yield response

        case BuiltRequest.Prepare(target) =>
          for {
            body <- ZIO.attempt {
              target match {
                case BuiltRequest.PrepareTarget.StatementId(id) => prepareBody(id)
                case BuiltRequest.PrepareTarget.Sql(sql)        => prepareBody(sql)
              }
            }.mapError(TarantoolError.CodecError)
            response <- send(RequestCode.Prepare, body)
          } yield response
      }

    private def resolveSpace(space: SpaceRef): IO[TarantoolError, Int] =
      space match {
        case SpaceRef.Id(id)     => ZIO.succeed(id)
        case SpaceRef.Name(name) => schemaMetaManager.getSpaceMeta(name).map(_.spaceId)
      }

    private def resolveIndex(space: SpaceRef, index: IndexRef): IO[TarantoolError, (Int, Int)] =
      (space, index) match {
        case (SpaceRef.Id(spaceId), IndexRef.Id(indexId)) =>
          ZIO.succeed((spaceId, indexId))
        case (SpaceRef.Name(spaceName), IndexRef.Name(indexName)) =>
          schemaMetaManager.getIndexMeta(spaceName, indexName).map(meta => (meta.spaceId, meta.indexId))
        case (SpaceRef.Id(_), IndexRef.Name(_)) | (SpaceRef.Name(_), IndexRef.Id(_)) =>
          ZIO.fail(
            TarantoolError.IncompleteRequest(
              "Space and index must both be referenced by id or both by name"
            )
          )
      }

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
