package zio.tarantool.impl

import zio._
import zio.tarantool.msgpack.Encoder.{longEncoder, stringEncoder}
import zio.tarantool.msgpack._
import zio.tarantool.protocol.{IteratorCode, Key, OperationCode}
import zio.tarantool.{TarantoolClient, TarantoolConnection, TarantoolOperation}

final class TarantoolClientLive(connection: TarantoolConnection.Service) extends TarantoolClient.Service {
  override def ping(): Task[TarantoolOperation] = for {
    response <- send(OperationCode.Ping, Map.empty)
  } yield response

  override def select(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray): Task[TarantoolOperation] =
    for {
      body <- ZIO.effect(selectBody(spaceId, indexId, limit, offset, iterator, key))
      response <- send(OperationCode.Select, body)
    } yield response

  override def insert(spaceId: Int, tuple: MpArray): Task[TarantoolOperation] = for {
    body <- ZIO.effect(insertBody(spaceId, tuple))
    response <- send(OperationCode.Insert, body)
  } yield response

  override def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[TarantoolOperation] = for {
    body <- ZIO.effect(updateBody(spaceId, indexId, key, tuple))
    response <- send(OperationCode.Update, body)
  } yield response

  override def delete(spaceId: Int, indexId: Int, key: MpArray): Task[TarantoolOperation] = for {
    body <- ZIO.effect(deleteBody(spaceId, indexId, key))
    response <- send(OperationCode.Delete, body)
  } yield response

  override def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[TarantoolOperation] = for {
    body <- ZIO.effect(upsertBody(spaceId, indexId, key, tuple))
    response <- send(OperationCode.Upsert, body)
  } yield response

  override def replace(spaceId: Int, tuple: MpArray): Task[TarantoolOperation] = for {
    body <- ZIO.effect(replaceBody(spaceId, tuple))
    response <- send(OperationCode.Replace, body)
  } yield response

  override def call(functionName: String, tuple: MpArray): Task[TarantoolOperation] = for {
    body <- ZIO.effect(callBody(functionName, tuple))
    response <- send(OperationCode.Call, body)
  } yield response

  override def eval(expression: String, tuple: MpArray): Task[TarantoolOperation] = for {
    body <- ZIO.effect(evalBody(expression, tuple))
    response <- send(OperationCode.Eval, body)
  } yield response

  private def send(op: OperationCode, body: Map[Long, MessagePack]): Task[TarantoolOperation] =
    connection.send(op, body)

  private def selectBody(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray)(
    implicit longEncoder: Encoder[Long]
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> longEncoder.encodeUnsafe(spaceId),
    Key.Index.value -> longEncoder.encodeUnsafe(indexId),
    Key.Limit.value -> longEncoder.encodeUnsafe(limit),
    Key.Offset.value -> longEncoder.encodeUnsafe(offset),
    Key.Iterator.value -> longEncoder.encodeUnsafe(iterator.value),
    Key.Key.value -> key
  )

  private def insertBody(spaceId: Int, tuple: MpArray)(
    implicit longEncoder: Encoder[Long]
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> longEncoder.encodeUnsafe(spaceId),
    Key.Tuple.value -> tuple
  )

  private def updateBody(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray)(
    implicit longEncoder: Encoder[Long]
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> longEncoder.encodeUnsafe(spaceId),
    Key.Index.value -> longEncoder.encodeUnsafe(indexId),
    Key.Key.value -> key,
    Key.Tuple.value -> tuple
  )

  private def deleteBody(spaceId: Int, indexId: Int, tuple: MpArray)(
    implicit longEncoder: Encoder[Long]
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> longEncoder.encodeUnsafe(spaceId),
    Key.Index.value -> longEncoder.encodeUnsafe(indexId),
    Key.Tuple.value -> tuple
  )

  private def upsertBody(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray)(
    implicit longEncoder: Encoder[Long]
  ): Map[Long, MessagePack] = Map(
    Key.Space.value -> longEncoder.encodeUnsafe(spaceId),
    Key.Index.value -> longEncoder.encodeUnsafe(indexId),
    Key.Key.value -> key,
    Key.Tuple.value -> tuple
  )

  private def replaceBody(
    spaceId: Int,
    tuple: MpArray
  )(implicit longEncoder: Encoder[Long]): Map[Long, MessagePack] =
    Map(
      Key.Space.value -> longEncoder.encodeUnsafe(spaceId),
      Key.Tuple.value -> tuple
    )

  private def callBody(
    functionName: String,
    tuple: MpArray
  )(implicit stringEncoder: Encoder[String]): Map[Long, MessagePack] = Map(
    Key.Function.value -> stringEncoder.encodeUnsafe(functionName),
    Key.Tuple.value -> tuple
  )

  private def evalBody(
    expression: String,
    tuple: MpArray
  )(implicit stringEncoder: Encoder[String]): Map[Long, MessagePack] = Map(
    Key.Expression.value -> stringEncoder.encodeUnsafe(expression),
    Key.Tuple.value -> tuple
  )
}
