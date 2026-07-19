package zio.tarantool.internal

import com.dimafeng.testcontainers.GenericContainer
import _root_.zio._
import _root_.zio.durationInt
import _root_.zio.Clock
import zio.tarantool.TarantoolError.AuthError
import zio.tarantool.{AuthInfo, BaseLayers, TarantoolConfig, TarantoolError}
import zio.tarantool.protocol.{MessagePackPacket, RequestCode, ResponseCode, ResponseType, TarantoolRequest}
import _root_.zio.test._
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect.{sequential, timeout}

object TarantoolConnectionSpec extends ZIOSpecDefault with BaseLayers {
  private val connectAndCommunicate =
    test("Create new connection then send and receive message") {
      for {
        _ <- TarantoolConnection.sendRequest(TarantoolRequest(RequestCode.Ping, 1, Map.empty))
        responseOpt <- TarantoolConnection.receive().take(1).runHead
        response <- ZIO.fromOption(responseOpt)
        responseType <- MessagePackPacket.responseType(response)
      } yield assertTrue(responseType == ResponseType.PingResponse)
    }

  private val failOnIncorrectAuthInfo = test("fail on incorrect auth info") {
    val layer = createTestSpecificLayer(Some(AuthInfo("random", "random")))

    val task = for {
      _ <- TarantoolConnection.sendRequest(TarantoolRequest(RequestCode.Ping, 1, Map.empty))
    } yield ()

    // Tarantool 2.11+ uses a generic message to avoid user enumeration (was Error(45) "User '...' is not found" in 2.6)
    assertZIO(task.provideLayer(layer).exit)(
      fails(
        equalTo(
          AuthError("User not found or supplied credentials are invalid", ResponseCode.Error(47))
        )
      )
    )
  }

  private val unsecuredSpecs = suite("TarantoolConnection without auth")(
    connectAndCommunicate.provideLayer(createTestSpecificLayer().orDie)
  ).provideSomeLayerShared[TestEnvironment](tarantoolLayer)

  private val securedSpecs = suite("TarantoolConnection with auth")(
    connectAndCommunicate.provideLayer(
      createTestSpecificLayer(Some(AuthInfo("username", "password"))).orDie
    ),
    failOnIncorrectAuthInfo
  ).provideSomeLayerShared[TestEnvironment](tarantoolSecuredLayer)

  override def spec: Spec[TestEnvironment, Any] =
    suite("TarantoolConnection")(
      unsecuredSpecs,
      securedSpecs
    ) @@ sequential @@ timeout(60.seconds)

  private def createTestSpecificLayer(
    authInfo: Option[AuthInfo] = None
  ): ZLayer[GenericContainer, TarantoolError, TarantoolConnection.Service] = {
    val clock = ZLayer.succeed[Clock](Clock.ClockLive)
    val syncId = syncIdProviderLayer
    val requestHandler = requestHandlerLayer

    val config = ZLayer {
      ZIO.serviceWith[GenericContainer] { container =>
        val cfg = TarantoolConfig(
          host = container.container.getHost,
          port = container.container.getMappedPort(3301)
        )

        cfg.copy(authInfo = authInfo)
      }
    }

    (clock ++ requestHandler ++ syncId ++ config) >>> TarantoolConnection.live
  }
}
