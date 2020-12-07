package zio.tarantool

import java.util.concurrent.TimeUnit

import zio.test.assert
import zio.test.Assertion._
import zio.{Has, ZLayer}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.TarantoolConnection.TarantoolConnection
import zio.tarantool.TarantoolContainer.Tarantool
import zio.tarantool.msgpack.MpFixArray
import zio.test.{DefaultRunnableSpec, ZSpec, suite, testM}

object TarantoolClientSpec extends DefaultRunnableSpec {
  val tarantoolLayer: ZLayer[Any, Nothing, Tarantool] = Blocking.live >>> TarantoolContainer.tarantool()
  val configLayer: ZLayer[Tarantool, Nothing, Has[ClientConfig]] = ZLayer.fromService(container =>
    ClientConfig(
      host = container.container.getHost,
      port = container.container.getMappedPort(3301)
    )
  )
  val tarantoolConnectionLayer: ZLayer[Tarantool, Throwable, TarantoolConnection] = configLayer >>> TarantoolConnection.live
  val tarantoolClientLayer: ZLayer[Any with Tarantool, Throwable, TarantoolClient] = tarantoolConnectionLayer >>> TarantoolClient.live
  val testEnv: ZLayer[Any, Throwable, Clock with TarantoolClient] = Clock.live ++ (tarantoolLayer >>> tarantoolClientLayer)

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Tarantool spec") {
      testM("Create space using eval") {
        for {
          _ <- TarantoolClient.eval("box.schema.create_space('test', {if_not_exists = true})", MpFixArray(Vector.empty))
          response <- TarantoolClient.eval("return box.space.test.id", MpFixArray(Vector.empty))
          _ <- TarantoolClient.eval("return box.space.test.id", MpFixArray(Vector.empty))
          result <- response.promise.await.timeout(zio.duration.Duration(5, TimeUnit.SECONDS))
        } yield {
          println(response)
          println(result)

          assert(true)(isTrue)
        }
      }
    }.provideCustomLayer(testEnv.orDie)
}
