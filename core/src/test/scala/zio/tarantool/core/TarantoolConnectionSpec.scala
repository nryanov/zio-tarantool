package zio.tarantool.core

import zio._
import zio.duration._
import zio.clock.Clock
import zio.tarantool.BaseLayers
import zio.tarantool.protocol.{MessagePackPacket, RequestCode, ResponseType, TarantoolRequest}
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.{sequential, timeout}

object TarantoolConnectionSpec extends DefaultRunnableSpec with BaseLayers {
  private val sharedLayer = configLayer ++ Clock.live ++ loggingLayer ++ syncIdProviderLayer
  private val sharedSecuredLayer =
    configSecuredLayer ++ Clock.live ++ loggingLayer ++ syncIdProviderLayer

  private val connectAndCommunicate =
    testM("Create new connection then send and receive message") {
      for {
        pingRequest <- TarantoolRequest.createPacket(
          TarantoolRequest(RequestCode.Ping, 1, Map.empty)
        )
        _ <- TarantoolConnection.sendRequest(pingRequest)
        responseOpt <- TarantoolConnection.receive().take(1).runHead
        response <- ZIO.fromOption(responseOpt)
        responseType <- MessagePackPacket.responseType(response)
      } yield assert(responseType)(equalTo(ResponseType.PingResponse))
    }

  private val unsecuredSpecs = suite("TarantoolConnection without auth")(
    connectAndCommunicate
  ).provideSomeLayer(TarantoolConnection.live.orDie).provideSomeLayerShared(sharedLayer)

  private val securedSpecs = suite("TarantoolConnection with auth")(
    connectAndCommunicate
  ).provideSomeLayer(TarantoolConnection.live.orDie).provideSomeLayerShared(sharedSecuredLayer)

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TarantoolConnection")(
      unsecuredSpecs,
      securedSpecs
    ) @@ sequential @@ timeout(5 seconds)
}
