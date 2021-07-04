package zio.tarantool.core

import zio.test._
import zio.duration._
import zio.test.Assertion._
import zio.tarantool.msgpack.MpPositiveFixInt
import zio.test.TestAspect.{sequential, timeout}
import zio.tarantool.{BaseLayers, TarantoolError}
import zio.tarantool.protocol.{Header, RequestCode, TarantoolRequest, TarantoolResponse}

object RequestHandlerSpec extends DefaultRunnableSpec with BaseLayers {
  val request: TarantoolRequest = TarantoolRequest(
    RequestCode.Ping,
    1L,
    Map(
      Header.Sync.value -> MpPositiveFixInt(1),
      Header.SchemaId.value -> MpPositiveFixInt(1)
    )
  )

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("RequestHandler")(
      testM("should submit new request") {
        val result = for {
          _ <- RequestHandler.submitRequest(request)
          sentRequests <- RequestHandler.sentRequests
        } yield assert(sentRequests.size)(equalTo(1)) && assert(sentRequests.get(1L))(isSome)

        result.provideLayer(requestHandlerLayer)
      },
      testM("should fail on duplicate request") {
        val result = for {
          _ <- RequestHandler.submitRequest(request)
          _ <- RequestHandler.submitRequest(request)
        } yield ()

        assertM(result.provideLayer(requestHandlerLayer).run)(
          fails(equalTo(TarantoolError.DuplicateOperation(1)))
        )
      },
      testM("should complete request") {
        val response = TarantoolResponse(MpPositiveFixInt(1))

        val result = for {
          operation <- RequestHandler.submitRequest(request)
          _ <- RequestHandler.complete(1L, response)
          isDone <- operation.isDone
        } yield assert(isDone)(isTrue)

        result.provideLayer(requestHandlerLayer)
      },
      testM("should fail request") {
        val result = for {
          operation <- RequestHandler.submitRequest(request)
          _ <- RequestHandler.fail(1L, "some error", 0)
          doneStatus <- operation.response.await.run
        } yield assert(doneStatus.succeeded)(isFalse)

        result.provideLayer(requestHandlerLayer)
      },
      testM("should throw error on completing request if request does not exist") {
        val response = TarantoolResponse(MpPositiveFixInt(1))
        val result = for {
          _ <- RequestHandler.complete(1L, response)
        } yield ()

        assertM(result.provideLayer(requestHandlerLayer).run)(
          fails(
            equalTo(
              TarantoolError.NotFoundOperation("Operation 1 not found")
            )
          )
        )
      },
      testM("should throw error on failing request if request does not exist") {
        val result = for {
          _ <- RequestHandler.fail(1L, "some error", 0)
        } yield ()

        assertM(result.provideLayer(requestHandlerLayer).run)(
          fails(
            equalTo(
              TarantoolError.NotFoundOperation("Operation 1 not found")
            )
          )
        )
      },
      testM("should fail all requests before closed") {
        val result =
          for {
            op1 <- RequestHandler.submitRequest(request)
            op2 <- RequestHandler.submitRequest(request.copy(syncId = 2L))
            op3 <- RequestHandler.submitRequest(request.copy(syncId = 3L))
            sentRequests <- RequestHandler.sentRequests
            _ <- RequestHandler.close()
            emptyRequests <- RequestHandler.sentRequests
            doneStatus1 <- op1.response.await.run
            doneStatus2 <- op2.response.await.run
            doneStatus3 <- op3.response.await.run
          } yield assert(sentRequests.size)(equalTo(3)) &&
            assert(emptyRequests.size)(equalTo(0)) &&
            assert(Seq(doneStatus1, doneStatus2, doneStatus3).forall(!_.succeeded))(isTrue)

        result.provideLayer(requestHandlerLayer)
      }
    ) @@ sequential @@ timeout(5 seconds)
}
