package zio.tarantool.impl

import zio._
import zio.tarantool.msgpack.Encoder.{longEncoder, stringEncoder}
import zio.tarantool.msgpack._
import zio.tarantool.protocol.{IteratorCode, Key, OperationCode, TupleEncoder}
import zio.tarantool.{Logging, TarantoolClient, TarantoolConnection, TarantoolOperation}
import TarantoolClientLive._

final class TarantoolClientLive(connection: TarantoolConnection.Service)
    extends TarantoolClient.Service
    with Logging {
  override def ping(): Task[TarantoolOperation] = for {
    _ <- debug("Ping request")
    response <- send(OperationCode.Ping, Map.empty)
  } yield response

  override def select(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): Task[TarantoolOperation] =
    for {
      _ <- debug(s"Select request: $key")
      body <- ZIO.effect(selectBody(spaceId, indexId, limit, offset, iterator, key))
      response <- send(OperationCode.Select, body)
    } yield response

  override def select[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: A
  ): Task[TarantoolOperation] = for {
    encodedKey <- ZIO.effect(TupleEncoder[A].encodeUnsafe(key))
    response <- select(spaceId, indexId, limit, offset, iterator, encodedKey)
  } yield response

  override def insert(spaceId: Int, tuple: MpArray): Task[TarantoolOperation] = for {
    _ <- debug(s"Insert request: $tuple")
    body <- ZIO.effect(insertBody(spaceId, tuple))
    response <- send(OperationCode.Insert, body)
  } yield response

  override def insert[A: TupleEncoder](spaceId: Int, tuple: A): Task[TarantoolOperation] = for {
    encodedTuple <- ZIO.effect(TupleEncoder[A].encodeUnsafe(tuple))
    response <- insert(spaceId, encodedTuple)
  } yield response

  override def update(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    ops: MpArray
  ): Task[TarantoolOperation] = for {
    _ <- debug(s"Update request. Key: $key, operations: $ops")
    body <- ZIO.effect(updateBody(spaceId, indexId, key, ops))
    response <- send(OperationCode.Update, body)
  } yield response

  override def update[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A,
    tuple: B
  ): Task[TarantoolOperation] = for {
    encodedKey <- ZIO.effect(TupleEncoder[A].encodeUnsafe(key))
    encodedTuple <- ZIO.effect(TupleEncoder[B].encodeUnsafe(tuple))
    response <- update(spaceId, indexId, encodedKey, encodedTuple)
  } yield response

  override def delete(spaceId: Int, indexId: Int, key: MpArray): Task[TarantoolOperation] = for {
    _ <- debug(s"Delete request: $key")
    body <- ZIO.effect(deleteBody(spaceId, indexId, key))
    response <- send(OperationCode.Delete, body)
  } yield response

  override def delete[A: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    key: A
  ): Task[TarantoolOperation] = for {
    encodedKey <- ZIO.effect(TupleEncoder[A].encodeUnsafe(key))
    response <- delete(spaceId, indexId, encodedKey)
  } yield response

  override def upsert(
    spaceId: Int,
    indexId: Int,
    ops: MpArray,
    tuple: MpArray
  ): Task[TarantoolOperation] = for {
    _ <- debug(s"Upsert request. Operations: $ops, tuple: $tuple")
    body <- ZIO.effect(upsertBody(spaceId, indexId, ops, tuple))
    response <- send(OperationCode.Upsert, body)
  } yield response

  override def upsert[A: TupleEncoder, B: TupleEncoder](
    spaceId: Int,
    indexId: Int,
    ops: A,
    tuple: B
  ): Task[TarantoolOperation] = for {
    encodedKey <- ZIO.effect(TupleEncoder[A].encodeUnsafe(ops))
    encodedTuple <- ZIO.effect(TupleEncoder[B].encodeUnsafe(tuple))
    response <- upsert(spaceId, indexId, encodedKey, encodedTuple)
  } yield response

  override def replace(spaceId: Int, tuple: MpArray): Task[TarantoolOperation] = for {
    _ <- debug(s"Replace request: $tuple")
    body <- ZIO.effect(replaceBody(spaceId, tuple))
    response <- send(OperationCode.Replace, body)
  } yield response

  override def replace[A: TupleEncoder](spaceId: Int, tuple: A): Task[TarantoolOperation] = for {
    encodedTuple <- ZIO.effect(TupleEncoder[A].encodeUnsafe(tuple))
    response <- replace(spaceId, encodedTuple)
  } yield response

  override def call(functionName: String, tuple: MpArray): Task[TarantoolOperation] = for {
    _ <- debug(s"Call request: $functionName, args: $tuple")
    body <- ZIO.effect(callBody(functionName, tuple))
    response <- send(OperationCode.Call, body)
  } yield response

  override def call(functionName: String): Task[TarantoolOperation] =
    call(functionName, EmptyTuple)

  override def call[A: TupleEncoder](functionName: String, args: A): Task[TarantoolOperation] =
    for {
      encodedArgs <- ZIO.effect(TupleEncoder[A].encodeUnsafe(args))
      response <- call(functionName, encodedArgs)
    } yield response

  override def eval(expression: String, tuple: MpArray): Task[TarantoolOperation] = for {
    _ <- debug(s"Eval request: $expression, args: $tuple")
    body <- ZIO.effect(evalBody(expression, tuple))
    response <- send(OperationCode.Eval, body)
  } yield response

  override def eval(expression: String): Task[TarantoolOperation] =
    eval(expression, EmptyTuple)

  override def eval[A: TupleEncoder](expression: String, args: A): Task[TarantoolOperation] = for {
    encodedArgs <- ZIO.effect(TupleEncoder[A].encodeUnsafe(args))
    response <- eval(expression, encodedArgs)
  } yield response

  private def send(op: OperationCode, body: Map[Long, MessagePack]): Task[TarantoolOperation] =
    connection.send(op, body)
}

object TarantoolClientLive {
  private val EmptyTuple = MpFixArray(Vector.empty)

  private def selectBody(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    Key.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    Key.Limit.value -> Encoder[Long].encodeUnsafe(limit),
    Key.Offset.value -> Encoder[Long].encodeUnsafe(offset),
    Key.Iterator.value -> Encoder[Long].encodeUnsafe(iterator.value),
    Key.Key.value -> key
  )

  private def insertBody(spaceId: Int, tuple: MpArray): Map[Long, MessagePack] = Map(
    Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    Key.Tuple.value -> tuple
  )

  private def updateBody(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    Key.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    Key.Key.value -> key,
    Key.Tuple.value -> tuple
  )

  private def deleteBody(spaceId: Int, indexId: Int, tuple: MpArray): Map[Long, MessagePack] = Map(
    Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    Key.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    Key.Key.value -> tuple
  )

  private def upsertBody(
    spaceId: Int,
    indexId: Int,
    ops: MpArray,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    Key.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    Key.UpsertOps.value -> ops,
    Key.Tuple.value -> tuple
  )

  private def replaceBody(
    spaceId: Int,
    tuple: MpArray
  ): Map[Long, MessagePack] =
    Map(
      Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
      Key.Tuple.value -> tuple
    )

  private def callBody(
    functionName: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    Key.Function.value -> Encoder[String].encodeUnsafe(functionName),
    Key.Tuple.value -> tuple
  )

  private def evalBody(
    expression: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    Key.Expression.value -> Encoder[String].encodeUnsafe(expression),
    Key.Tuple.value -> tuple
  )
}
