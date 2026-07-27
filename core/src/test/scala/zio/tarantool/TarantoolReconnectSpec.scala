package zio.tarantool

import com.dimafeng.testcontainers.GenericContainer
import _root_.zio._
import _root_.zio.test._
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect._
import zio.tarantool.internal.TarantoolConnection
import zio.tarantool.protocol.{RequestCode, TarantoolRequest}

object TarantoolReconnectSpec extends ZIOSpecDefault with BaseLayers {

  private def restartContainer: ZIO[GenericContainer, Throwable, Unit] =
    ZIO.serviceWithZIO[GenericContainer] { container =>
      ZIO.attemptBlocking {
        container.container.getDockerClient.restartContainerCmd(container.container.getContainerId).exec()
      }.unit
    }

  private val reconnectConfig = ClientConfig(
    reconnectEnabled = true,
    reconnectRetries = 10,
    reconnectIntervalMillis = 200,
    reconnectWaitTimeoutMillis = 10000,
    useSchemaMetaCache = false
  )

  private val disabledConfig = ClientConfig(
    reconnectEnabled = false,
    useSchemaMetaCache = false
  )

  private def makeConfig(
    clientConfig: ClientConfig,
    authInfo: Option[AuthInfo]
  ): ZLayer[GenericContainer, Nothing, TarantoolConfig] =
    ZLayer {
      ZIO.serviceWith[GenericContainer] { container =>
        TarantoolConfig(
          connectionConfig = ConnectionConfig(
            host = container.container.getHost,
            port = container.container.getMappedPort(3301),
            connectionTimeoutMillis = 2000,
            retries = 0
          ),
          clientConfig = clientConfig,
          authInfo = authInfo
        )
      }
    }

  private def clientLayer(
    clientConfig: ClientConfig,
    authInfo: Option[AuthInfo] = None
  ): ZLayer[GenericContainer, Nothing, TarantoolClient.Service] = {
    val clock = ZLayer.succeed[Clock](Clock.ClockLive)
    (clock ++ makeConfig(clientConfig, authInfo)) >>> TarantoolClient.live.orDie
  }

  private def connectionEnv(
    clientConfig: ClientConfig,
    authInfo: Option[AuthInfo] = None,
    container: ZLayer[Any, Nothing, GenericContainer] = tarantoolLayer
  ): ZLayer[Any, Nothing, TarantoolConnection.Service] = {
    val clock = ZLayer.succeed[Clock](Clock.ClockLive)
    val connection =
      (clock ++ syncIdProviderLayer ++ requestHandlerLayer ++ makeConfig(clientConfig, authInfo)) >>>
        TarantoolConnection.live.orDie
    container >>> connection
  }

  private def fullLayer(
    clientConfig: ClientConfig,
    authInfo: Option[AuthInfo] = None
  ): ZLayer[Any, Nothing, TarantoolClient.Service with GenericContainer] =
    tarantoolLayer >+> clientLayer(clientConfig, authInfo)

  private val reconnectAfterForcedDisconnect = test("reconnects after forced socket close") {
    for {
      _ <- TarantoolConnection.sendRequest(TarantoolRequest(RequestCode.Ping, 1L, Map.empty)).flatMap(_.response.await)
      _ <- TarantoolConnection.forceReconnect()
      _ <- TarantoolConnection
        .sendRequest(TarantoolRequest(RequestCode.Ping, 2L, Map.empty))
        .flatMap(_.response.await)
        .retry(Schedule.spaced(200.millis) && Schedule.recurs(50))
    } yield assertCompletes
  }

  private val reconnectWithAuthAfterForcedDisconnect =
    test("reconnects and re-authenticates after forced socket close") {
      for {
        _ <- TarantoolConnection
          .sendRequest(TarantoolRequest(RequestCode.Ping, 1L, Map.empty))
          .flatMap(_.response.await)
        _ <- TarantoolConnection.forceReconnect()
        _ <- TarantoolConnection
          .sendRequest(TarantoolRequest(RequestCode.Ping, 2L, Map.empty))
          .flatMap(_.response.await)
          .retry(Schedule.spaced(200.millis) && Schedule.recurs(50))
      } yield assertCompletes
    }

  private val failInFlightOnDisconnect = test("fails in-flight requests with ConnectionLost") {
    for {
      promise <- TarantoolClient.eval.expression("require('fiber').sleep(60) return 1").run
      _ <- ZIO.sleep(500.millis)
      _ <- restartContainer
      exit <- promise.await.exit
    } yield assert(exit)(
      fails(isSubtype[TarantoolError.ConnectionLost](anything))
    )
  }

  private val failWhenReconnectDisabled = test("fails permanently when reconnect is disabled") {
    for {
      _ <- TarantoolClient.ping().flatMap(_.await)
      _ <- restartContainer
      _ <- ZIO.sleep(2.seconds)
      exit <- TarantoolClient.ping().flatMap(_.await).exit
    } yield assert(exit)(
      fails(
        isSubtype[TarantoolError.ReconnectFailed](anything) ||
          isSubtype[TarantoolError.ConnectionLost](anything) ||
          isSubtype[TarantoolError.Timeout](anything)
      )
    )
  }

  override def spec: Spec[TestEnvironment, Any] =
    suite("TarantoolClient reconnect")(
      reconnectAfterForcedDisconnect.provideLayer(connectionEnv(reconnectConfig)),
      reconnectWithAuthAfterForcedDisconnect.provideLayer(
        connectionEnv(reconnectConfig, Some(AuthInfo("username", "password")), tarantoolSecuredLayer)
      ),
      failInFlightOnDisconnect.provideLayer(fullLayer(reconnectConfig)),
      failWhenReconnectDisabled.provideLayer(fullLayer(disabledConfig))
    ) @@ sequential @@ withLiveClock @@ timeout(180.seconds)
}
