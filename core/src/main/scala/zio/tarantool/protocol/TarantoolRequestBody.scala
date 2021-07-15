package zio.tarantool.protocol

import org.msgpack.value.Value
import zio.tarantool.codec.Encoder

object TarantoolRequestBody {
  def selectBody(
    spaceId: Int,
    indexId: Int,
    limit: Int,
    offset: Int,
    iterator: IteratorCode,
    key: Value
  ): Map[Long, Value] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encode(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encode(indexId),
    RequestBodyKey.Limit.value -> Encoder[Int].encode(limit),
    RequestBodyKey.Offset.value -> Encoder[Int].encode(offset),
    RequestBodyKey.Iterator.value -> Encoder[Int].encode(iterator.value),
    RequestBodyKey.Key.value -> key
  )

  def insertBody(spaceId: Int, tuple: Value): Map[Long, Value] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encode(spaceId),
    RequestBodyKey.Tuple.value -> tuple
  )

  def updateBody(
    spaceId: Int,
    indexId: Int,
    key: Value,
    tuple: Value
  ): Map[Long, Value] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encode(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encode(indexId),
    RequestBodyKey.Key.value -> key,
    RequestBodyKey.Tuple.value -> tuple
  )

  def deleteBody(spaceId: Int, indexId: Int, key: Value): Map[Long, Value] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encode(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encode(indexId),
    RequestBodyKey.Key.value -> key
  )

  def upsertBody(
    spaceId: Int,
    indexId: Int,
    ops: Value,
    tuple: Value
  ): Map[Long, Value] = Map(
    RequestBodyKey.Space.value -> Encoder[Int].encode(spaceId),
    RequestBodyKey.Index.value -> Encoder[Int].encode(indexId),
    RequestBodyKey.UpsertOps.value -> ops,
    RequestBodyKey.Tuple.value -> tuple
  )

  def replaceBody(
    spaceId: Int,
    tuple: Value
  ): Map[Long, Value] =
    Map(
      RequestBodyKey.Space.value -> Encoder[Int].encode(spaceId),
      RequestBodyKey.Tuple.value -> tuple
    )

  def callBody(
    functionName: String,
    tuple: Value
  ): Map[Long, Value] = Map(
    RequestBodyKey.Function.value -> Encoder[String].encode(functionName),
    RequestBodyKey.Tuple.value -> tuple
  )

  def evalBody(
    expression: String,
    tuple: Value
  ): Map[Long, Value] = Map(
    RequestBodyKey.Expression.value -> Encoder[String].encode(expression),
    RequestBodyKey.Tuple.value -> tuple
  )

  def executeBody(
    statementId: Int,
    sqlBind: Value,
    options: Value
  ): Map[Long, Value] = Map(
    RequestSqlBodyKey.StatementId.value -> Encoder[Int].encode(statementId),
    RequestSqlBodyKey.SqlBind.value -> sqlBind,
    RequestSqlBodyKey.Options.value -> options
  )

  def executeBody(
    sqlText: String,
    sqlBind: Value,
    options: Value
  ): Map[Long, Value] = Map(
    RequestSqlBodyKey.SqlText.value -> Encoder[String].encode(sqlText),
    RequestSqlBodyKey.SqlBind.value -> sqlBind,
    RequestSqlBodyKey.Options.value -> options
  )

  def prepareBody(
    statementId: Int
  ): Map[Long, Value] = Map(
    RequestSqlBodyKey.StatementId.value -> Encoder[Int].encode(statementId)
  )

  def prepareBody(
    sqlText: String
  ): Map[Long, Value] = Map(
    RequestSqlBodyKey.SqlText.value -> Encoder[String].encode(sqlText)
  )

  def authBody(username: String, authMechanism: String, scramble: Array[Byte]): Map[Long, Value] =
    Map(
      RequestBodyKey.Username.value -> Encoder[String].encode(username),
      RequestBodyKey.Tuple.value -> Encoder[Vector[Array[Byte]]].encode(
        Vector(authMechanism.getBytes, scramble)
      )
    )
}
