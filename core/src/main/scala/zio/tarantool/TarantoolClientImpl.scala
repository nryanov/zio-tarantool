package zio.tarantool

import zio._
import zio.tarantool.TarantoolClient.TarantoolClientService
import zio.tarantool.TarantoolClientImpl.EmptyBody
import zio.tarantool.TarantoolConnection.TarantoolConnectionService
import zio.tarantool.msgpack._
import zio.tarantool.msgpack.Implicits.RichMessagePack
import zio.tarantool.msgpack.Encoder.{longEncoder, mapEncoder, messagePackEncoder, stringEncoder}
import zio.tarantool.protocol.{IteratorCode, Key, MessagePackPacket, OperationCode}

final class TarantoolClientImpl(tarantoolConnection: TarantoolConnection.Service) extends TarantoolClient.Service {
  override def ping(): Task[MessagePackPacket] = for {
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Ping, None, EmptyBody))
    response <- writeAndRead(packet)
  } yield response

  override def select(spaceId: Int, indexId: Int, limit: Int, offset: Int, iterator: IteratorCode, key: MpArray): Task[MessagePackPacket] =
    for {
      body <- ZIO.effect(selectBody(spaceId, indexId, limit, offset, iterator, key))
      packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Select, None, body))
      response <- writeAndRead(packet)
    } yield response

  override def insert(spaceId: Int, tuple: MpArray): Task[MessagePackPacket] = for {
    body <- ZIO.effect(insertBody(spaceId, tuple))
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Insert, None, body))
    response <- writeAndRead(packet)
  } yield response

  override def update(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePackPacket] = for {
    body <- ZIO.effect(updateBody(spaceId, indexId, key, tuple))
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Update, None, body))
    response <- writeAndRead(packet)
  } yield response

  override def delete(spaceId: Int, indexId: Int, key: MpArray): Task[MessagePackPacket] = for {
    body <- ZIO.effect(deleteBody(spaceId, indexId, key))
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Delete, None, body))
    response <- writeAndRead(packet)
  } yield response

  override def upsert(spaceId: Int, indexId: Int, key: MpArray, tuple: MpArray): Task[MessagePackPacket] = for {
    body <- ZIO.effect(upsertBody(spaceId, indexId, key, tuple))
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Upsert, None, body))
    response <- writeAndRead(packet)
  } yield response

  override def replace(spaceId: Int, tuple: MpArray): Task[MessagePackPacket] = for {
    body <- ZIO.effect(replaceBody(spaceId, tuple))
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Replace, None, body))
    response <- writeAndRead(packet)
  } yield response

  override def call(functionName: String, tuple: MpArray): Task[MessagePackPacket] = for {
    body <- ZIO.effect(callBody(functionName, tuple))
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Call, None, body))
    response <- writeAndRead(packet)
  } yield response

  override def eval(expression: String, tuple: MpArray): Task[MessagePackPacket] = for {
    body <- ZIO.effect(evalBody(expression, tuple))
    packet <- ZIO.effectTotal(tarantoolConnection.createPacket(OperationCode.Eval, None, body))
    response <- writeAndRead(packet)
  } yield response

  private def writeAndRead(packet: MessagePackPacket): Task[MessagePackPacket] =
    tarantoolConnection.writePacket(packet) *> tarantoolConnection.readPacket()

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

object TarantoolClientImpl {
  private val EmptyBody: MpMap = MpFixMap(Map.empty)

  def live: ZLayer[TarantoolConnectionService, Nothing, TarantoolClientService] =
    ZLayer.fromService(new TarantoolClientImpl(_))
}
