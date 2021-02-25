package zio.tarantool.protocol

import zio.tarantool.msgpack.{Encoder, MessagePack, MpArray}

object TarantoolRequestBody {
  def selectBody(
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

  def insertBody(spaceId: Int, tuple: MpArray): Map[Long, MessagePack] = Map(
    Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    Key.Tuple.value -> tuple
  )

  def updateBody(
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

  def deleteBody(spaceId: Int, indexId: Int, tuple: MpArray): Map[Long, MessagePack] = Map(
    Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    Key.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    Key.Key.value -> tuple
  )

  def upsertBody(
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

  def replaceBody(
    spaceId: Int,
    tuple: MpArray
  ): Map[Long, MessagePack] =
    Map(
      Key.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
      Key.Tuple.value -> tuple
    )

  def callBody(
    functionName: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    Key.Function.value -> Encoder[String].encodeUnsafe(functionName),
    Key.Tuple.value -> tuple
  )

  def evalBody(
    expression: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    Key.Expression.value -> Encoder[String].encodeUnsafe(expression),
    Key.Tuple.value -> tuple
  )
}
