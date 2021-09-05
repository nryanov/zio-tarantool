package zio.tarantool.internal

import com.dimafeng.testcontainers.GenericContainer
import zio._
import zio.duration._
import zio.clock.Clock
import zio.tarantool.TarantoolContainer.Tarantool
import zio.tarantool.TarantoolError.AuthError
import zio.tarantool.internal.TarantoolConnection.TarantoolConnection
import zio.tarantool.{AuthInfo, BaseLayers, TarantoolConfig, TarantoolError}
import zio.tarantool.protocol.{MessagePackPacket, RequestCode, ResponseCode, ResponseType, TarantoolRequest}
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.{sequential, timeout}

object TarantoolConnectionSpec extends DefaultRunnableSpec with BaseLayers {
  private val connectAndCommunicate =
    testM("Create new connection then send and receive message") {
      for {
        _ <- TarantoolConnection.sendRequest(TarantoolRequest(RequestCode.Ping, 1, Map.empty))
        responseOpt <- TarantoolConnection.receive().take(1).runHead
        response <- ZIO.fromOption(responseOpt)
        responseType <- MessagePackPacket.responseType(response)
      } yield assert(responseType)(equalTo(ResponseType.PingResponse))
    }

  private val failOnIncorrectAuthInfo = testM("fail on incorrect auth info") {
    val layer = createTestSpecificLayer(Some(AuthInfo("random", "random")))

    val task = for {
      _ <- TarantoolConnection.sendRequest(TarantoolRequest(RequestCode.Ping, 1, Map.empty))
    } yield ()

    assertM(task.provideLayer(layer).run)(
      fails(equalTo(AuthError("User 'random' is not found", ResponseCode.Error(45))))
    )
  }

  private val unsecuredSpecs = suite("TarantoolConnection without auth")(
    connectAndCommunicate.provideLayer(createTestSpecificLayer().orDie)
  ).provideSomeLayerShared(tarantoolLayer)

  private val securedSpecs = suite("TarantoolConnection with auth")(
    connectAndCommunicate.provideLayer(
      createTestSpecificLayer(Some(AuthInfo("username", "password"))).orDie
    ),
    failOnIncorrectAuthInfo
  ).provideSomeLayerShared(tarantoolSecuredLayer)

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TarantoolConnection")(
      unsecuredSpecs,
      securedSpecs
    ) @@ sequential @@ timeout(60 seconds)

  private def createTestSpecificLayer(
    authInfo: Option[AuthInfo] = None
  ): ZLayer[Tarantool, TarantoolError, TarantoolConnection] = {
    val tarantool = ZLayer.requires[Tarantool]
    val clock = Clock.live
    val syncId = syncIdProviderLayer
    val requestHandler = requestHandlerLayer

    val config = ZLayer.fromService[GenericContainer, TarantoolConfig] { container =>
      val cfg = TarantoolConfig(
        host = container.container.getHost,
        port = container.container.getMappedPort(3301)
      )

      cfg.copy(authInfo = authInfo)
    }

    (tarantool ++ clock ++ requestHandler ++ syncId ++ config) >>> TarantoolConnection.live
  }
}
