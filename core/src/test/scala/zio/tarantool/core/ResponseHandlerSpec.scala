package zio.tarantool.core

import java.time.Duration

import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.mock.Expectation._
import zio.test.TestAspect.{sequential, timeout}
import zio.tarantool.mock.{RequestHandlerMock, TarantoolConnectionMock}
import zio.tarantool.core.ResponseHandler.ResponseHandler
import zio.tarantool.msgpack.{MpFixString, MpPositiveFixInt}
import zio.tarantool.protocol.{FieldKey, MessagePackPacket, ResponseCode}
import zio.tarantool.{BaseLayers, TarantoolConfig, TarantoolError}

object ResponseHandlerSpec extends DefaultRunnableSpec with BaseLayers {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("BackgroundReader")(
      testM("should die if channel is in blocking mode") {
        val configLayer = ZLayer.succeed(TarantoolConfig())
        val connectionMock = TarantoolConnectionMock.IsBlocking(value(true))
        val packetManager = PacketManager.live
        val requestHandler = loggingLayer >>> RequestHandler.live
        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
          (configLayer ++ loggingLayer ++ connectionMock ++ packetManager ++ requestHandler) >>> ResponseHandler.live

        val result = for {
          _ <- ResponseHandler.start()
        } yield ()

        assertM(result.run.provideLayer(layer))(
          dies(isSubtype[IllegalArgumentException](anything))
        )
      },
      testM("should fail with error when bytes read < 0") {
        val configLayer = ZLayer.succeed(TarantoolConfig())
        val packetManager = PacketManager.live
        val requestHandler = loggingLayer >>> RequestHandler.live
        val connectionMock = TarantoolConnectionMock.IsBlocking(
          value(false)
        ) ++ TarantoolConnectionMock.RegisterSelector(
          anything,
          value(())
        ) ++ TarantoolConnectionMock.Read(anything, value(-1))

        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
          (configLayer ++ loggingLayer ++ connectionMock ++ packetManager ++ requestHandler) >>> ResponseHandler.live

        val result = for {
          fiber <- ResponseHandler.start()
          _ <- fiber.join
        } yield ()

        assertM(result.run.provideLayer(layer))(
          fails(
            equalTo(
              TarantoolError.MessagePackPacketReadError("Error while reading message pack packet")
            )
          )
        )
      },
      testM("should complete operation") {
        val configLayer = ZLayer.succeed(TarantoolConfig())
        val packetManager = PacketManager.live
        val requestHandlerMock = RequestHandlerMock.Complete(anything, value(()))
        val connectionLayer = TarantoolConnection.test

        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
          (configLayer ++ loggingLayer ++ connectionLayer ++ packetManager ++ requestHandlerMock) >>> ResponseHandler.live

        val messagePackPacket = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> MpPositiveFixInt(1),
            FieldKey.SchemaId.value -> MpPositiveFixInt(1),
            FieldKey.Code.value -> MpPositiveFixInt(ResponseCode.Success.value)
          ),
          Map(
            FieldKey.Data.value -> MpPositiveFixInt(1)
          )
        )

        val result = for {
          _ <- ResponseHandler.complete(messagePackPacket)
        } yield ()

        assertM(result.provideLayer(layer))(isUnit)
      },
      testM("should fail operation") {
        val configLayer = ZLayer.succeed(TarantoolConfig())
        val packetManager = PacketManager.live
        val requestHandlerMock = RequestHandlerMock.Fail(anything, value(()))
        val connectionLayer = TarantoolConnection.test

        val layer: ZLayer[Any, TarantoolError, ResponseHandler] =
          (configLayer ++ loggingLayer ++ connectionLayer ++ packetManager ++ requestHandlerMock) >>> ResponseHandler.live

        val messagePackPacket = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> MpPositiveFixInt(1),
            FieldKey.SchemaId.value -> MpPositiveFixInt(1),
            FieldKey.Code.value -> MpPositiveFixInt(0x8001)
          ),
          Map(
            FieldKey.Error.value -> MpFixString("error")
          )
        )

        val result = for {
          _ <- ResponseHandler.complete(messagePackPacket)
        } yield ()

        assertM(result.provideLayer(layer))(isUnit)
      }
    ) @@ sequential @@ timeout(Duration.ofSeconds(5))
}
