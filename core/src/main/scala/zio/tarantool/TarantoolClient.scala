package zio.tarantool

import zio._
import zio.clock.Clock
import zio.logging._
import zio.macros.accessible
import zio.tarantool.core._
import zio.tarantool.msgpack._
import zio.tarantool.msgpack.MpArray
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.TarantoolRequestBody._
import zio.tarantool.protocol.{IteratorCode, OperationCode, TarantoolOperation}

@accessible[TarantoolClient.Service]
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

  val live: ZLayer[Has[TarantoolConfig] with Logging with Clock, Throwable, TarantoolClient] =
    ZLayer.fromServiceManaged[TarantoolConfig, Logging with Clock, Throwable, Service] { cfg =>
      make(cfg)
    }

  def make(config: TarantoolConfig): ZManaged[Clock with Logging, Throwable, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      connection <- TarantoolConnection.make(config).tapM(_.connect())
      syncIdProvider <- SyncIdProvider.make()
      packetManager <- PacketManager.make()
      requestHandler <- RequestHandler.make()
      responseHandler <- ResponseHandler
        .make(connection, packetManager, requestHandler)
        .tapM(_.start())
      queuedWriter <- SocketChannelQueuedWriter.make(config, connection).tapM(_.start())
      schemaMetaManager <- SchemaMetaManager.make(
        config,
        requestHandler,
        queuedWriter,
        syncIdProvider
      )
//        .tapM(_.refresh)
      communicationInterceptor <- CommunicationInterceptor.make(
        schemaMetaManager,
        requestHandler,
        responseHandler,
        queuedWriter,
        syncIdProvider
      )
    } yield new Live(logger, communicationInterceptor)

  private[this] val EmptyTuple = MpFixArray(Vector.empty)

  private[this] final class Live(
    logger: Logger[String],
    communicationInterceptor: CommunicationInterceptor.Service
  ) extends TarantoolClient.Service {
    override def ping(): IO[TarantoolError, TarantoolOperation] = for {
      response <- send(OperationCode.Ping, Map.empty)
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
        response <- send(OperationCode.Select, body)
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

    override def insert(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation] =
      for {
        body <- ZIO.effect(insertBody(spaceId, tuple)).mapError(TarantoolError.CodecError)
        response <- send(OperationCode.Insert, body)
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

    override def update(
      spaceId: Int,
      indexId: Int,
      key: MpArray,
      ops: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO.effect(updateBody(spaceId, indexId, key, ops)).mapError(TarantoolError.CodecError)
      response <- send(OperationCode.Update, body)
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

    override def delete(
      spaceId: Int,
      indexId: Int,
      key: MpArray
    ): IO[TarantoolError, TarantoolOperation] = for {
      body <- ZIO.effect(deleteBody(spaceId, indexId, key)).mapError(TarantoolError.CodecError)
      response <- send(OperationCode.Delete, body)
    } yield response

    override def delete[A: TupleEncoder](
      spaceId: Int,
      indexId: Int,
      key: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedKey <- TupleEncoder[A].encodeM(key)
      response <- delete(spaceId, indexId, encodedKey)
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
      response <- send(OperationCode.Upsert, body)
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

    override def replace(spaceId: Int, tuple: MpArray): IO[TarantoolError, TarantoolOperation] =
      for {
        body <- ZIO.effect(replaceBody(spaceId, tuple)).mapError(TarantoolError.CodecError)
        response <- send(OperationCode.Replace, body)
      } yield response

    override def replace[A: TupleEncoder](
      spaceId: Int,
      tuple: A
    ): IO[TarantoolError, TarantoolOperation] = for {
      encodedTuple <- TupleEncoder[A].encodeM(tuple)
      response <- replace(spaceId, encodedTuple)
    } yield response

    override def call(
      functionName: String,
      tuple: MpArray
    ): IO[TarantoolError, TarantoolOperation] =
      for {
        body <- ZIO.effect(callBody(functionName, tuple)).mapError(TarantoolError.CodecError)
        response <- send(OperationCode.Call, body)
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
        response <- send(OperationCode.Eval, body)
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

    private def send(
      op: OperationCode,
      body: Map[Long, MessagePack]
    ): IO[TarantoolError, TarantoolOperation] =
      communicationInterceptor.submitRequest(op, body)
  }
}
