//package zio.tarantool.core
//
//import java.time.Duration
//
//import zio.{Has, Ref, ZIO, ZLayer}
//import zio.clock.Clock
//import zio.tarantool.{BaseLayers, TarantoolConfig, TarantoolError}
//import zio.tarantool.mock.{PacketManagerMock, SocketChannelProviderMock}
//import zio.tarantool.protocol.MessagePackPacket
//import zio.test._
//import zio.test.Assertion._
//import zio.test.mock.Expectation._
//import zio.test.TestAspect.{sequential, timeout}
//
//object ResponseHandlerSpec extends DefaultRunnableSpec with BaseLayers {
//  val backgroundReader = ResponseHandler.live
//  val baseLayers = Clock.live ++ loggingLayer >+> RequestHandler.live ++ PacketManager.live
//
//  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
//    suite("BackgroundReader")(
//      testM("should die if channel is in blocking mode") {
//        val cfg = ZLayer.succeed(TarantoolConfig())
//        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live))
//        val socketChannelProviderMock =
//          socketMock.IsBlocking(
//            value(true)
//          )
//        val layer =
//          (cfg ++ socketChannelProviderMock ++ baseLayers) >>> backgroundReader
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
//        val cfg = ZLayer.succeed(TarantoolConfig())
//        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live))
//        val socketChannelProviderMock = {
//          socketMock.IsBlocking(
//            value(false)
//          ) ++ socketMock.RegisterSelector(
//            anything,
//            value(())
//          ) ++ socketMock.Read(
//            anything,
//            value(-1)
//          )
//        }
//        val layer =
//          (cfg ++ socketChannelProviderMock ++ baseLayers) >>> backgroundReader
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
//      }
//    ) @@ sequential @@ timeout(Duration.ofSeconds(5))
//}
