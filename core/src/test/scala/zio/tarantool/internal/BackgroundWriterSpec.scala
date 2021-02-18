package zio.tarantool.internal

import java.nio.ByteBuffer
import java.time.Duration

import zio.clock.Clock
import zio.{Has, ZIO, ZLayer}
import zio.tarantool.{ClientConfig, TarantoolConfig}
import zio.tarantool.mock.SocketChannelProviderMock
import zio.test.Assertion._
import zio.test.TestAspect.{sequential, timeout}
import zio.test._
import zio.test.mock.Expectation._

object BackgroundWriterSpec extends DefaultRunnableSpec {
  val backgroundWriter = BackgroundWriter.live

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("BackgroundWriter")(
      testM("should direct write data") {
        val cfg = ZLayer.succeed(TarantoolConfig())
        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live))
        val mock =
          socketMock.Write(
            anything,
            value(4)
          )
        val layer = (cfg ++ mock ++ Clock.live) >>> backgroundWriter
        val buffer = ByteBuffer.allocate(4).putInt(1)
        buffer.flip()

        val result = for {
          r <- BackgroundWriter.write(buffer)
        } yield r

        assertM(result.provideLayer(layer))(isUnit)
      },
      testM("should send request to queue") {
        val cfg = ZLayer.succeed(
          TarantoolConfig().copy(clientConfig = ClientConfig(writeTimeoutMillis = 100))
        )
        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live), writeDelayMs = 500)
        val mock =
          socketMock.Write(
            anything,
            value(4)
          )
        val layer = (cfg ++ mock ++ Clock.live) >>> backgroundWriter

        val bufferOne = ByteBuffer.allocate(1).put(1.byteValue)
        val bufferTwo = ByteBuffer.allocate(2).put(1.byteValue).put(2.byteValue)
        bufferOne.flip()
        bufferTwo.flip()

        val result = for {
          f1 <- BackgroundWriter.write(bufferOne).fork
          f2 <- BackgroundWriter.write(bufferTwo).fork
          _ <- f1.join
          _ <- f2.join
          size <- BackgroundWriter.requestQueue().flatMap(_.size)
        } yield size

        assertM(result.provideLayer(layer))(equalTo(1))
      },
      testM("should send request to queue and read it using background fiber") {
        val cfg = ZLayer.succeed(
          TarantoolConfig().copy(clientConfig = ClientConfig(writeTimeoutMillis = 100))
        )
        val socketMock = new SocketChannelProviderMock(Has(Clock.Service.live), writeDelayMs = 500)
        val mock =
          socketMock.Write(
            anything,
            value(4)
          )
        val layer = (cfg ++ mock ++ Clock.live) >>> backgroundWriter

        val bufferOne = ByteBuffer.allocate(1).put(1.byteValue)
        val bufferTwo = ByteBuffer.allocate(2).put(1.byteValue).put(2.byteValue)
        bufferOne.flip()
        bufferTwo.flip()

        val result = for {
          f1 <- BackgroundWriter.write(bufferOne).fork
          f2 <- BackgroundWriter.write(bufferTwo).fork
          _ <- f1.join
          _ <- f2.join
          size <- BackgroundWriter.requestQueue().flatMap(_.size)
          fiber <- BackgroundWriter.start()
          afterRead <- BackgroundWriter.requestQueue().flatMap(_.size).repeatWhile(_ != 0)
          _ <- fiber.interrupt //todo: ?
        } yield assert(size)(equalTo(1)) && assert(afterRead)(equalTo(0))

        result.provideLayer(layer)
      }
    ) @@ sequential @@ timeout(Duration.ofSeconds(5))
}
