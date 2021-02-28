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
    FieldKey.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    FieldKey.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    FieldKey.Limit.value -> Encoder[Long].encodeUnsafe(limit),
    FieldKey.Offset.value -> Encoder[Long].encodeUnsafe(offset),
    FieldKey.Iterator.value -> Encoder[Long].encodeUnsafe(iterator.value),
    FieldKey.Key.value -> key
  )

  def insertBody(spaceId: Int, tuple: MpArray): Map[Long, MessagePack] = Map(
    FieldKey.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    FieldKey.Tuple.value -> tuple
  )

  def updateBody(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    FieldKey.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    FieldKey.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    FieldKey.Key.value -> key,
    FieldKey.Tuple.value -> tuple
  )

  def deleteBody(spaceId: Int, indexId: Int, tuple: MpArray): Map[Long, MessagePack] = Map(
    FieldKey.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    FieldKey.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    FieldKey.Key.value -> tuple
  )

  def upsertBody(
    spaceId: Int,
    indexId: Int,
    ops: MpArray,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    FieldKey.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
    FieldKey.Index.value -> Encoder[Long].encodeUnsafe(indexId),
    FieldKey.UpsertOps.value -> ops,
    FieldKey.Tuple.value -> tuple
  )

  def replaceBody(
    spaceId: Int,
    tuple: MpArray
  ): Map[Long, MessagePack] =
    Map(
      FieldKey.Space.value -> Encoder[Long].encodeUnsafe(spaceId),
      FieldKey.Tuple.value -> tuple
    )

  def callBody(
    functionName: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    FieldKey.Function.value -> Encoder[String].encodeUnsafe(functionName),
    FieldKey.Tuple.value -> tuple
  )

  def evalBody(
    expression: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    FieldKey.Expression.value -> Encoder[String].encodeUnsafe(expression),
    FieldKey.Tuple.value -> tuple
  )
}
