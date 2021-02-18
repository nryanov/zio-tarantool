package zio.tarantool.internal

import java.time.Duration

import zio.{Has, Ref, ZIO, ZLayer}
import zio.clock.Clock
import zio.tarantool.internal.impl.BackgroundReaderLive
import zio.tarantool.{TarantoolConfig, TarantoolError}
import zio.tarantool.mock.{PacketManagerMock, SocketChannelProviderMock}
import zio.tarantool.protocol.MessagePackPacket
import zio.test._
import zio.test.Assertion._
import zio.test.mock.Expectation._
import zio.test.TestAspect.{sequential, timeout}

object BackgroundReaderSpec extends DefaultRunnableSpec {
  val backgroundReader = BackgroundReader.live

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("BackgroundReader")(
      testM("should die if channel is in blocking mode") {
        val cfg = ZLayer.succeed(TarantoolConfig())
        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live))
        val socketChannelProviderMock =
          socketMock.IsBlocking(
            value(true)
          )
        val layer =
          (cfg ++ socketChannelProviderMock ++ PacketManager.live ++ Clock.live) >>> backgroundReader
        val completionHandler: MessagePackPacket => ZIO[Any, TarantoolError.IOError, Unit] =
          _ => ZIO.succeed(())

        val result = for {
          _ <- BackgroundReader.start(completionHandler)
        } yield ()

        assertM(result.run.provideLayer(layer))(
          dies(isSubtype[IllegalArgumentException](anything))
        )
      },
      testM("should fail with error when bytes read < 0") {
        val cfg = ZLayer.succeed(TarantoolConfig())
        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live))
        val socketChannelProviderMock = {
          socketMock.IsBlocking(
            value(false)
          ) ++ socketMock.RegisterSelector(
            anything,
            value(())
          ) ++ socketMock.Read(
            anything,
            value(-1)
          )
        }
        val layer =
          (cfg ++ socketChannelProviderMock ++ PacketManager.live ++ Clock.live) >>> backgroundReader
        val completionHandler: MessagePackPacket => ZIO[Any, TarantoolError.IOError, Unit] =
          _ => ZIO.succeed(())

        val result = for {
          fiber <- BackgroundReader.start(completionHandler)
          _ <- fiber.join
        } yield ()

        assertM(result.run.provideLayer(layer))(
          fails(equalTo(BackgroundReaderLive.MessagePackPacketReadError))
        )
      }
//      testM("should call completion handler") {
//        val cfg = ZLayer.succeed(TarantoolConfig())
//        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live))
//        val socketChannelProviderMock =
//          socketMock.IsBlocking(
//            value(false)
//          ) ++ socketMock.RegisterSelector(
//            anything,
//            value(())
//          ) ++ socketMock.Read(
//            anything,
//            value(0)
//          )
//        val packetManagerMock =
//          PacketManagerMock.DecodeToMessagePackPacket(anything, value(MessagePackPacket(Map.empty)))
//        val layer =
//          (cfg ++ socketChannelProviderMock ++ packetManagerMock ++ Clock.live) >>> backgroundReader
//
//        (for {
//          ref <- Ref.make(false)
//          completionHandler = (_: MessagePackPacket) => ref.set(true)
//          _ <- BackgroundReader.start(completionHandler)
//          result <- ref.get.repeatWhile(!_)
//        } yield assert(result)(isTrue)).provideLayer(layer)
//      }
    ) @@ sequential @@ timeout(Duration.ofSeconds(5))
}
