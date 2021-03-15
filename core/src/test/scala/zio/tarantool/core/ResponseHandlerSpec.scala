//package zio.tarantool.core
//
//import java.time.Duration
//
//import zio._
//import zio.logging.{Logger, Logging}
//import zio.tarantool.core.DelayedQueue.DelayedQueue
//import zio.tarantool.core.RequestHandler.RequestHandler
//import zio.test._
//import zio.test.Assertion._
//import zio.test.mock.Expectation._
//import zio.test.TestAspect.{sequential, timeout}
//import zio.tarantool.mock.{RequestHandlerMock, TarantoolConnectionMock}
//import zio.tarantool.core.ResponseHandler.{Live, ResponseHandler}
//import zio.tarantool.core.TarantoolConnection.TarantoolConnection
//import zio.tarantool.msgpack.{MpFixString, MpPositiveFixInt}
//import zio.tarantool.protocol.{Header, MessagePackPacket, ResponseBodyKey, ResponseCode}
//import zio.tarantool.{BaseLayers, TarantoolConfig, TarantoolError}
//
//object ResponseHandlerSpec extends DefaultRunnableSpec with BaseLayers {
//  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
//    suite("BackgroundReader")(
//      testM("should die if channel is in blocking mode") {
//        val configLayer = ZLayer.succeed(TarantoolConfig())
//        val connectionMock = TarantoolConnectionMock.IsBlocking(value(true))
//        val requestHandler = loggingLayer >>> RequestHandler.live
//        val delayedQueueLayer = DelayedQueue.test
//        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
//          (configLayer ++ loggingLayer ++ connectionMock ++ requestHandler ++ delayedQueueLayer) >>> notStartedResponseHandlerLayer
//
//        val result = for {
//          _ <- ResponseHandler.start()
//        } yield ()
//
//        assertM(result.run.provideLayer(layer))(
//          dies(isSubtype[IllegalArgumentException](anything))
//        )
//      },
//      testM("should fail with error when bytes read < 0") {
//        val configLayer = ZLayer.succeed(TarantoolConfig())
//        val requestHandler = loggingLayer >>> RequestHandler.live
//        val delayedQueueLayer = DelayedQueue.test
//        val connectionMock = TarantoolConnectionMock.IsBlocking(
//          value(false)
//        ) ++ TarantoolConnectionMock.RegisterSelector(
//          anything,
//          value(())
//        ) ++ TarantoolConnectionMock.Read(anything, value(-1))
//
//        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
//          (configLayer ++ loggingLayer ++ connectionMock ++ requestHandler ++ delayedQueueLayer) >>> notStartedResponseHandlerLayer
//
//        val result = for {
//          fiber <- ResponseHandler.start()
//          _ <- fiber.join
//        } yield ()
//
//        assertM(result.run.provideLayer(layer))(
//          fails(
//            equalTo(
//              TarantoolError.MessagePackPacketReadError("Error while reading message pack packet")
//            )
//          )
//        )
//      },
//      testM("should complete operation") {
//        val configLayer = ZLayer.succeed(TarantoolConfig())
//        val requestHandlerMock = RequestHandlerMock.Complete(anything, value(()))
//        val connectionLayer = TarantoolConnection.test
//        val delayedQueueLayer = DelayedQueue.test
//
//        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
//          (configLayer ++ loggingLayer ++ connectionLayer ++ requestHandlerMock ++ delayedQueueLayer) >>> notStartedResponseHandlerLayer
//
//        val messagePackPacket = MessagePackPacket(
//          Map(
//            Header.Sync.value -> MpPositiveFixInt(1),
//            Header.SchemaId.value -> MpPositiveFixInt(1),
//            Header.Code.value -> MpPositiveFixInt(ResponseCode.Success.value)
//          ),
//          Map(
//            ResponseBodyKey.Data.value -> MpPositiveFixInt(1)
//          )
//        )
//
//        val result = for {
//          _ <- ResponseHandler.complete(messagePackPacket)
//        } yield ()
//
//        assertM(result.provideLayer(layer))(isUnit)
//      },
//      testM("should fail operation") {
//        val configLayer = ZLayer.succeed(TarantoolConfig())
//        val requestHandlerMock = RequestHandlerMock.Fail(anything, value(()))
//        val delayedQueueLayer = DelayedQueue.test
//        val connectionLayer = TarantoolConnection.test
//
//        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
//          (configLayer ++ loggingLayer ++ connectionLayer ++ requestHandlerMock ++ delayedQueueLayer) >>> notStartedResponseHandlerLayer
//
//        val messagePackPacket = MessagePackPacket(
//          Map(
//            Header.Sync.value -> MpPositiveFixInt(1),
//            Header.SchemaId.value -> MpPositiveFixInt(1),
//            Header.Code.value -> MpPositiveFixInt(0x8001)
//          ),
//          Map(
//            ResponseBodyKey.Error.value -> MpFixString("error")
//          )
//        )
//
//        val result = for {
//          _ <- ResponseHandler.complete(messagePackPacket)
//        } yield ()
//
//        assertM(result.provideLayer(layer))(isUnit)
//      }
//    ) @@ sequential @@ timeout(Duration.ofSeconds(5))
//
//  private val notStartedResponseHandlerLayer: ZLayer[
//    RequestHandler with TarantoolConnection with DelayedQueue with Logging,
//    Nothing,
//    ResponseHandler
//  ] =
//    ZLayer.fromManaged(
//      ZManaged.make(
//        (for {
//          logger <- ZIO.service[Logger[String]]
//          connection <- ZIO.service[TarantoolConnection.Service]
//          requestHandler <- ZIO.service[RequestHandler.Service]
//          delayedQueue <- ZIO.service[DelayedQueue.Service]
//        } yield new Live(
//          logger,
//          connection,
//          requestHandler,
//          delayedQueue,
//          ExecutionContextManager.singleThreaded()
//        ))
//      )(_.close().orDie)
//    )
//}
