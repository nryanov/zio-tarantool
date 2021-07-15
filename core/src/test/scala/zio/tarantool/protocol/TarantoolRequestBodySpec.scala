package zio.tarantool.protocol

import org.msgpack.value.impl.{
  ImmutableArrayValueImpl,
  ImmutableLongValueImpl,
  ImmutableStringValueImpl
}
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
      key = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Index.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Limit.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Offset.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Iterator.value -> new ImmutableLongValueImpl(2),
          RequestBodyKey.Key.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createInsertBody = test("create insert body") {
    val body = TarantoolRequestBody.insertBody(
      spaceId = 1,
      tuple = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Tuple.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createUpdateBody = test("create update body") {
    val body = TarantoolRequestBody.updateBody(
      spaceId = 1,
      indexId = 1,
      key = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1))),
      tuple = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Index.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Key.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          ),
          RequestBodyKey.Tuple.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createDeleteBody = test("create delete body") {
    val body = TarantoolRequestBody.deleteBody(
      spaceId = 1,
      indexId = 1,
      key = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Index.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Key.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createUpsertBody = test("create upsert body") {
    val body = TarantoolRequestBody.upsertBody(
      spaceId = 1,
      indexId = 1,
      ops = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1))),
      tuple = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Index.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.UpsertOps.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          ),
          RequestBodyKey.Tuple.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createReplaceBody = test("create replace body") {
    val body = TarantoolRequestBody.replaceBody(
      spaceId = 1,
      tuple = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Space.value -> new ImmutableLongValueImpl(1),
          RequestBodyKey.Tuple.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createCallBody = test("create call body") {
    val body = TarantoolRequestBody.callBody(
      functionName = "test",
      tuple = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Function.value -> new ImmutableStringValueImpl("test"),
          RequestBodyKey.Tuple.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createEvalBody = test("create eval body") {
    val body = TarantoolRequestBody.evalBody(
      expression = "test",
      tuple = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestBodyKey.Expression.value -> new ImmutableStringValueImpl("test"),
          RequestBodyKey.Tuple.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createExecuteBodyStatementId = test("create execute body -- statement id") {
    val body = TarantoolRequestBody.executeBody(
      statementId = 1,
      sqlBind = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1))),
      options = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestSqlBodyKey.StatementId.value -> new ImmutableLongValueImpl(1),
          RequestSqlBodyKey.SqlBind.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          ),
          RequestSqlBodyKey.Options.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
        )
      )
    )
  }

  private val createExecuteBodySqlText = test("create execute body -- sql text") {
    val body = TarantoolRequestBody.executeBody(
      sqlText = "test",
      sqlBind = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1))),
      options = new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1)))
    )

    assert(body)(
      equalTo(
        Map(
          RequestSqlBodyKey.SqlText.value -> new ImmutableStringValueImpl("test"),
          RequestSqlBodyKey.SqlBind.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          ),
          RequestSqlBodyKey.Options.value -> new ImmutableArrayValueImpl(
            Array(new ImmutableLongValueImpl(1))
          )
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
          RequestSqlBodyKey.StatementId.value -> new ImmutableLongValueImpl(1)
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
          RequestSqlBodyKey.SqlText.value -> new ImmutableStringValueImpl("test")
        )
      )
    )
  }

  private val createAuthBody = test("create auth body") {
    val body = TarantoolRequestBody.authBody(
      username = "test",
      "value",
      scramble = Array.empty
    )

    // ImmutableBinaryValueImpl uses ref equality to compare Array[Byte]
    assert(body.get(RequestBodyKey.Username.value))(
      isSome(equalTo(new ImmutableStringValueImpl("test")))
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
