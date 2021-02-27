package zio.tarantool

import zio._
import zio.logging.Logger
import zio.macros.accessible
import zio.tarantool.msgpack.MpArray
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.TarantoolRequestBody._
import zio.tarantool.msgpack._
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.core.TarantoolCommunicationInterceptor
import zio.tarantool.core.TarantoolCommunicationInterceptor.TarantoolCommunicationInterceptor
import zio.tarantool.protocol.{IteratorCode, OperationCode, TarantoolOperation}

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

  val live: ZLayer[TarantoolCommunicationInterceptor, Nothing, TarantoolClient] = ???
//    ZLayer.fromServiceManaged[TarantoolCommunicationInterceptor.Service, Any, Nothing, Service](
//      make
//    )
//
//  def make(service: TarantoolCommunicationInterceptor.Service): ZManaged[Any, Nothing, Service] =
//    ZManaged.succeed(service).map(new Live(_))

  private[this] val EmptyTuple = MpFixArray(Vector.empty)

  private[this] final class Live(
    logger: Logger[String],
    service: TarantoolCommunicationInterceptor.Service
  ) extends TarantoolClient.Service {
    override def ping(): IO[TarantoolError, TarantoolOperation] = for {
      _ <- logger.debug("Ping request")
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
        _ <- logger.debug(s"Select request: $key")
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
        _ <- logger.debug(s"Insert request: $tuple")
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
      _ <- logger.debug(s"Update request. Key: $key, operations: $ops")
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
      _ <- logger.debug(s"Delete request: $key")
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
      _ <- logger.debug(s"Upsert request. Operations: $ops, tuple: $tuple")
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
        _ <- logger.debug(s"Replace request: $tuple")
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
        _ <- logger.debug(s"Call request: $functionName, args: $tuple")
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
        _ <- logger.debug(s"Eval request: $expression, args: $tuple")
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
      service.submitRequest(op, body)
  }
}
