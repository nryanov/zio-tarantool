package zio.tarantool.impl

import zio._
import zio.tarantool.impl.TarantoolClientLive.EmptyBody
import zio.tarantool.msgpack.Encoder.{longEncoder, mapEncoder, messagePackEncoder, stringEncoder}
import zio.tarantool.msgpack.Implicits.RichMessagePack
import zio.tarantool.msgpack._
import zio.tarantool.protocol.{IteratorCode, Key, OperationCode}
import zio.tarantool.{TarantoolClient, TarantoolConnection}

final class TarantoolClientLive(connection: TarantoolConnection.Service) extends TarantoolClient.Service {
  override def ping(): Task[MessagePack] = for {
    response <- writeAndRead(OperationCode.Ping, EmptyBody)
  } yield response

  override def select(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray): Task[MessagePack] =
    for {
      body <- ZIO.effect(selectBody(spaceId, indexId, limit, offset, iterator, key))
      response <- writeAndRead(OperationCode.Select, body)
    } yield response

  override def insert(spaceId: Int, tuple: MpArray): Task[MessagePack] = for {
    body <- ZIO.effect(insertBody(spaceId, tuple))
    response <- writeAndRead(OperationCode.Insert, body)
  } yield response

  override def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePack] = for {
    body <- ZIO.effect(updateBody(spaceId, indexId, key, tuple))
    response <- writeAndRead(OperationCode.Update, body)
  } yield response

  override def delete(spaceId: Int, indexId: Int, key: MpArray): Task[MessagePack] = for {
    body <- ZIO.effect(deleteBody(spaceId, indexId, key))
    response <- writeAndRead(OperationCode.Delete, body)
  } yield response

  override def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePack] = for {
    body <- ZIO.effect(upsertBody(spaceId, indexId, key, tuple))
    response <- writeAndRead(OperationCode.Upsert, body)
  } yield response

  override def replace(spaceId: Int, tuple: MpArray): Task[MessagePack] = for {
    body <- ZIO.effect(replaceBody(spaceId, tuple))
    response <- writeAndRead(OperationCode.Replace, body)
  } yield response

  override def call(functionName: String, tuple: MpArray): Task[MessagePack] = for {
    body <- ZIO.effect(callBody(functionName, tuple))
    response <- writeAndRead(OperationCode.Call, body)
  } yield response

  override def eval(expression: String, tuple: MpArray): Task[MessagePack] = for {
    body <- ZIO.effect(evalBody(expression, tuple))
    response <- writeAndRead(OperationCode.Eval, body)
  } yield response

  private def writeAndRead(op: OperationCode, body: MpMap): Task[MessagePack] =
    connection.write(op, body) *> connection.read()

  private def selectBody(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray)(
    implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]],
    longEncoder: Encoder[Long]
  ): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Space.value) -> longEncoder.encodeUnsafe(spaceId),
        longEncoder.encodeUnsafe(Key.Index.value) -> longEncoder.encodeUnsafe(indexId),
        longEncoder.encodeUnsafe(Key.Limit.value) -> longEncoder.encodeUnsafe(limit),
        longEncoder.encodeUnsafe(Key.Offset.value) -> longEncoder.encodeUnsafe(offset),
        longEncoder.encodeUnsafe(Key.Iterator.value) -> longEncoder.encodeUnsafe(iterator.value),
        longEncoder.encodeUnsafe(Key.Key.value) -> key
      )
    )
    .toMap

  private def insertBody(spaceId: Int, tuple: MpArray)(
    implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]],
    longEncoder: Encoder[Long]
  ): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Space.value) -> longEncoder.encodeUnsafe(spaceId),
        longEncoder.encodeUnsafe(Key.Tuple.value) -> tuple
      )
    )
    .toMap

  private def updateBody(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray)(
    implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]],
    longEncoder: Encoder[Long]
  ): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Space.value) -> longEncoder.encodeUnsafe(spaceId),
        longEncoder.encodeUnsafe(Key.Index.value) -> longEncoder.encodeUnsafe(indexId),
        longEncoder.encodeUnsafe(Key.Key.value) -> key,
        longEncoder.encodeUnsafe(Key.Tuple.value) -> tuple
      )
    )
    .toMap

  private def deleteBody(spaceId: Int, indexId: Int, tuple: MpArray)(
    implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]],
    longEncoder: Encoder[Long]
  ): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Space.value) -> longEncoder.encodeUnsafe(spaceId),
        longEncoder.encodeUnsafe(Key.Index.value) -> longEncoder.encodeUnsafe(indexId),
        longEncoder.encodeUnsafe(Key.Tuple.value) -> tuple
      )
    )
    .toMap

  private def upsertBody(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray)(
    implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]],
    longEncoder: Encoder[Long]
  ): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Space.value) -> longEncoder.encodeUnsafe(spaceId),
        longEncoder.encodeUnsafe(Key.Index.value) -> longEncoder.encodeUnsafe(indexId),
        longEncoder.encodeUnsafe(Key.Key.value) -> key,
        longEncoder.encodeUnsafe(Key.Tuple.value) -> tuple
      )
    )
    .toMap

  private def replaceBody(
    spaceId: Int,
    tuple: MpArray
  )(implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]], longEncoder: Encoder[Long]): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Space.value) -> longEncoder.encodeUnsafe(spaceId),
        longEncoder.encodeUnsafe(Key.Tuple.value) -> tuple
      )
    )
    .toMap

  private def callBody(
    functionName: String,
    tuple: MpArray
  )(implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]], stringEncoder: Encoder[String]): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Function.value) -> stringEncoder.encodeUnsafe(functionName),
        longEncoder.encodeUnsafe(Key.Tuple.value) -> tuple
      )
    )
    .toMap

  private def evalBody(
    expression: String,
    tuple: MpArray
  )(implicit mapEncoder: Encoder[Map[MessagePack, MessagePack]], stringEncoder: Encoder[String]): MpMap = mapEncoder
    .encodeUnsafe(
      Map(
        longEncoder.encodeUnsafe(Key.Expression.value) -> stringEncoder.encodeUnsafe(expression),
        longEncoder.encodeUnsafe(Key.Tuple.value) -> tuple
      )
    )
    .toMap
}

object TarantoolClientLive {
  private val EmptyBody: MpMap = MpFixMap(Map.empty)
}
