package zio.tarantool.protocol

import zio.tarantool.msgpack.{MpFixArray, MpFixString, MpPositiveFixInt}
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.sequential

object TarantoolRequestBodySpec extends DefaultRunnableSpec {
  private val createSelectBody = test("create select body") {
    val body = TarantoolRequestBody.selectBody(
      spaceId = 1,
      indexId = 1,
      limit = 1,
      offset = 1,
      iterator = IteratorCode.All,
      key = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> MpPositiveFixInt(1),
          RequestBodyKey.Index.value -> MpPositiveFixInt(1),
          RequestBodyKey.Limit.value -> MpPositiveFixInt(1),
          RequestBodyKey.Offset.value -> MpPositiveFixInt(1),
          RequestBodyKey.Iterator.value -> MpPositiveFixInt(2),
          RequestBodyKey.Key.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createInsertBody = test("create insert body") {
    val body = TarantoolRequestBody.insertBody(
      spaceId = 1,
      tuple = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> MpPositiveFixInt(1),
          RequestBodyKey.Tuple.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createUpdateBody = test("create update body") {
    val body = TarantoolRequestBody.updateBody(
      spaceId = 1,
      indexId = 1,
      key = MpFixArray(Vector(MpPositiveFixInt(1))),
      tuple = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> MpPositiveFixInt(1),
          RequestBodyKey.Index.value -> MpPositiveFixInt(1),
          RequestBodyKey.Key.value -> MpFixArray(Vector(MpPositiveFixInt(1))),
          RequestBodyKey.Tuple.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createDeleteBody = test("create delete body") {
    val body = TarantoolRequestBody.deleteBody(
      spaceId = 1,
      indexId = 1,
      key = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> MpPositiveFixInt(1),
          RequestBodyKey.Index.value -> MpPositiveFixInt(1),
          RequestBodyKey.Key.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createUpsertBody = test("create upsert body") {
    val body = TarantoolRequestBody.upsertBody(
      spaceId = 1,
      indexId = 1,
      ops = MpFixArray(Vector(MpPositiveFixInt(1))),
      tuple = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> MpPositiveFixInt(1),
          RequestBodyKey.Index.value -> MpPositiveFixInt(1),
          RequestBodyKey.UpsertOps.value -> MpFixArray(Vector(MpPositiveFixInt(1))),
          RequestBodyKey.Tuple.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createReplaceBody = test("create replace body") {
    val body = TarantoolRequestBody.replaceBody(
      spaceId = 1,
      tuple = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> MpPositiveFixInt(1),
          RequestBodyKey.Tuple.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createCallBody = test("create call body") {
    val body = TarantoolRequestBody.callBody(
      functionName = "test",
      tuple = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Function.value -> MpFixString("test"),
          RequestBodyKey.Tuple.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createEvalBody = test("create eval body") {
    val body = TarantoolRequestBody.evalBody(
      expression = "test",
      tuple = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Expression.value -> MpFixString("test"),
          RequestBodyKey.Tuple.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createExecuteBodyStatementId = test("create execute body -- statement id") {
    val body = TarantoolRequestBody.executeBody(
      statementId = 1,
      sqlBind = MpFixArray(Vector(MpPositiveFixInt(1))),
      options = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestSqlBodyKey.StatementId.value -> MpPositiveFixInt(1),
          RequestSqlBodyKey.SqlBind.value -> MpFixArray(Vector(MpPositiveFixInt(1))),
          RequestSqlBodyKey.Options.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createExecuteBodySqlText = test("create execute body -- sql text") {
    val body = TarantoolRequestBody.executeBody(
      sqlText = "test",
      sqlBind = MpFixArray(Vector(MpPositiveFixInt(1))),
      options = MpFixArray(Vector(MpPositiveFixInt(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestSqlBodyKey.SqlText.value -> MpFixString("test"),
          RequestSqlBodyKey.SqlBind.value -> MpFixArray(Vector(MpPositiveFixInt(1))),
          RequestSqlBodyKey.Options.value -> MpFixArray(Vector(MpPositiveFixInt(1)))
        )
      )
    )
  }

  private val createPrepareBodyStatementId = test("create prepare body -- statement id") {
    val body = TarantoolRequestBody.prepareBody(
      statementId = 1
    )

    assert(body)(
      equalTo(
        Map(
          RequestSqlBodyKey.StatementId.value -> MpPositiveFixInt(1)
        )
      )
    )
  }

  private val createPrepareBodyStatementTest = test("create prepare body -- sql text") {
    val body = TarantoolRequestBody.prepareBody(
      sqlText = "test"
    )

    assert(body)(
      equalTo(
        Map(
          RequestSqlBodyKey.SqlText.value -> MpFixString("test")
        )
      )
    )
  }

  private val createAuthBody = test("create auth body") {
    val body = TarantoolRequestBody.authBody(
      username = "test",
      body = Vector.empty
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Username.value -> MpFixString("test"),
          RequestBodyKey.Tuple.value -> MpFixArray(Vector.empty)
        )
      )
    )
  }

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TarantoolRequestBody")(
      createSelectBody,
      createInsertBody,
      createUpdateBody,
      createDeleteBody,
      createUpsertBody,
      createReplaceBody,
      createCallBody,
      createEvalBody,
      createExecuteBodyStatementId,
      createExecuteBodySqlText,
      createPrepareBodyStatementId,
      createPrepareBodyStatementTest,
      createAuthBody
    ) @@ sequential
}
