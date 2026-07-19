package zio.tarantool.internal

import org.msgpack.value.impl._
import _root_.zio.test._
import _root_.zio.durationInt
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect.{sequential, timeout}
import zio.tarantool.{BaseLayers, TarantoolError}
import zio.tarantool.protocol.{Header, RequestCode, TarantoolRequest}

object RequestHandlerSpec extends ZIOSpecDefault with BaseLayers {
  val request: TarantoolRequest = TarantoolRequest(
    RequestCode.Ping,
    1L,
    Map(
      Header.Sync.value -> new ImmutableLongValueImpl(1),
      Header.SchemaId.value -> new ImmutableLongValueImpl(1)
    )
  )

  override def spec: Spec[TestEnvironment, Any] =
    suite("RequestHandler")(
      test("should submit new request") {
        val result = for {
          _ <- RequestHandler.submitRequest(request)
          sentRequests <- RequestHandler.sentRequests
        } yield assert(sentRequests.size)(equalTo(1)) && assert(sentRequests.get(1L))(isSome)

        result.provideLayer(requestHandlerLayer)
      },
      test("should fail on duplicate request") {
        val result = for {
          _ <- RequestHandler.submitRequest(request)
          _ <- RequestHandler.submitRequest(request)
        } yield ()

        assertZIO(result.provideLayer(requestHandlerLayer).exit)(
          fails(equalTo(TarantoolError.DuplicateOperation(1)))
        )
      },
      test("should complete request") {
        val result = for {
          operation <- RequestHandler.submitRequest(request)
          _ <- RequestHandler.complete(1L, new ImmutableLongValueImpl(1))
          isDone <- operation.isDone
        } yield assert(isDone)(isTrue)

        result.provideLayer(requestHandlerLayer)
      },
      test("should fail request") {
        val result = for {
          operation <- RequestHandler.submitRequest(request)
          _ <- RequestHandler.fail(1L, "some error", 0)
          doneStatus <- operation.response.await.exit
        } yield assert(doneStatus.isSuccess)(isFalse)

        result.provideLayer(requestHandlerLayer)
      },
      test("should throw error on completing request if request does not exist") {
        val result = for {
          _ <- RequestHandler.complete(1L, new ImmutableLongValueImpl(1))
        } yield ()

        assertZIO(result.provideLayer(requestHandlerLayer).exit)(
          fails(equalTo(TarantoolError.NotFoundOperation(1L)))
        )
      },
      test("should throw error on failing request if request does not exist") {
        val result = for {
          _ <- RequestHandler.fail(1L, "some error", 0)
        } yield ()

        assertZIO(result.provideLayer(requestHandlerLayer).exit)(
          fails(equalTo(TarantoolError.NotFoundOperation(1L)))
        )
      },
      test("should fail all requests before closed") {
        val result =
          for {
            op1 <- RequestHandler.submitRequest(request)
            op2 <- RequestHandler.submitRequest(request.copy(syncId = 2L))
            op3 <- RequestHandler.submitRequest(request.copy(syncId = 3L))
            sentRequests <- RequestHandler.sentRequests
            _ <- RequestHandler.close()
            emptyRequests <- RequestHandler.sentRequests
            doneStatus1 <- op1.response.await.exit
            doneStatus2 <- op2.response.await.exit
            doneStatus3 <- op3.response.await.exit
          } yield assert(sentRequests.size)(equalTo(3)) &&
            assert(emptyRequests.size)(equalTo(0)) &&
            assert(Seq(doneStatus1, doneStatus2, doneStatus3).forall(!_.isSuccess))(isTrue)

        result.provideLayer(requestHandlerLayer)
      }
    ) @@ sequential @@ timeout(5.seconds)
}
