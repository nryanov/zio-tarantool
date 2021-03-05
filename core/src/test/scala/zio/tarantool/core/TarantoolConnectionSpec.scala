package zio.tarantool.core

import zio.ZLayer
import zio.duration._
import zio.clock.Clock
import zio.tarantool.BaseLayers
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.{sequential, timeout}

object TarantoolConnectionSpec extends DefaultRunnableSpec with BaseLayers {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TarantoolConnection")(
      testM("isConnected should be true after successful connection") {
        val layer = TarantoolConnection.live

        val result = for {
          connected <- TarantoolConnection.isConnected
          isBlocking <- TarantoolConnection.isBlocking
          _ <- TarantoolConnection.close()
          notConnected <- TarantoolConnection.isConnected
        } yield assert(notConnected)(isFalse) &&
          assert(connected)(isTrue) &&
          assert(isBlocking)(isFalse)

        result.provideLayer(layer)
      }
    ).provideSomeLayerShared(configLayer ++ Clock.live ++ loggingLayer) @@ sequential @@ timeout(
      5 seconds
    )
}
