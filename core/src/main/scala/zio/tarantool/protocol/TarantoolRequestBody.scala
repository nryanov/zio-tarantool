package zio.tarantool.protocol

import scodec.bits.ByteVector
import zio.tarantool.msgpack.{MessagePack, MpArray}

object TarantoolRequestBody {
  def selectBody(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: MpArray
  ): Map[Long, MessagePack] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encodeUnsafe(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encodeUnsafe(indexId),
    RequestBodyKey.Limit.value -> Encoder[Int].encodeUnsafe(limit),
    RequestBodyKey.Offset.value -> Encoder[Int].encodeUnsafe(offset),
    RequestBodyKey.Iterator.value -> Encoder[Int].encodeUnsafe(iterator.value),
    RequestBodyKey.Key.value -> key
  )

  def insertBody(spaceId: Int, tuple: MpArray): Map[Long, MessagePack] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encodeUnsafe(spaceId),
    RequestBodyKey.Tuple.value -> tuple
  )

  def updateBody(
    spaceId: Int,
    indexId: Int,
    key: MpArray,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encodeUnsafe(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encodeUnsafe(indexId),
    RequestBodyKey.Key.value -> key,
    RequestBodyKey.Tuple.value -> tuple
  )

  def deleteBody(spaceId: Int, indexId: Int, key: MpArray): Map[Long, MessagePack] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encodeUnsafe(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encodeUnsafe(indexId),
    RequestBodyKey.Key.value -> key
  )

  def upsertBody(
    spaceId: Int,
    indexId: Int,
    ops: MpArray,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encodeUnsafe(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encodeUnsafe(indexId),
    RequestBodyKey.UpsertOps.value -> ops,
    RequestBodyKey.Tuple.value -> tuple
  )

  def replaceBody(
    spaceId: Int,
    tuple: MpArray
  ): Map[Long, MessagePack] =
    Map(
      RequestBodyKey.Space.value -> Encoder[Int].encodeUnsafe(spaceId),
      RequestBodyKey.Tuple.value -> tuple
    )

  def callBody(
    functionName: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    RequestBodyKey.Function.value -> Encoder[String].encodeUnsafe(functionName),
    RequestBodyKey.Tuple.value -> tuple
  )

  def evalBody(
    expression: String,
    tuple: MpArray
  ): Map[Long, MessagePack] = Map(
    RequestBodyKey.Expression.value -> Encoder[String].encodeUnsafe(expression),
    RequestBodyKey.Tuple.value -> tuple
  )

  def executeBody(
    statementId: Int,
    sqlBind: MpArray,
    options: MpArray
  ): Map[Long, MessagePack] = Map(
    RequestSqlBodyKey.StatementId.value -> Encoder[Int].encodeUnsafe(statementId),
    RequestSqlBodyKey.SqlBind.value -> sqlBind,
    RequestSqlBodyKey.Options.value -> options
  )

  def executeBody(
    sqlText: String,
    sqlBind: MpArray,
    options: MpArray
  ): Map[Long, MessagePack] = Map(
    RequestSqlBodyKey.SqlText.value -> Encoder[String].encodeUnsafe(sqlText),
    RequestSqlBodyKey.SqlBind.value -> sqlBind,
    RequestSqlBodyKey.Options.value -> options
  )

  def prepareBody(
    statementId: Int
  ): Map[Long, MessagePack] = Map(
    RequestSqlBodyKey.StatementId.value -> Encoder[Int].encodeUnsafe(statementId)
  )

  def prepareBody(
    sqlText: String
  ): Map[Long, MessagePack] = Map(
    RequestSqlBodyKey.SqlText.value -> Encoder[String].encodeUnsafe(sqlText)
  )

  def authBody(username: String, body: Vector[ByteVector]): Map[Long, MessagePack] = Map(
    RequestBodyKey.Username.value -> Encoder[String].encodeUnsafe(username),
    RequestBodyKey.Tuple.value -> Encoder[Vector[ByteVector]].encodeUnsafe(body)
  )
}
