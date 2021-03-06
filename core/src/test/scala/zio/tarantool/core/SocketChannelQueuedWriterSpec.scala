package zio.tarantool.core

import java.nio.ByteBuffer

import zio._
import zio.duration._
import zio.clock.Clock
import zio.logging.Logger
import zio.tarantool.core.SocketChannelQueuedWriter.SocketChannelQueuedWriter
import zio.tarantool.core.TarantoolConnection.TarantoolConnection
import zio.test._
import zio.test.Assertion._
import zio.test.mock.Expectation._
import zio.test.TestAspect.{sequential, timeout}
import zio.tarantool.mock.TarantoolConnectionMock
import zio.tarantool.msgpack.MpPositiveFixInt
import zio.tarantool.protocol.{Header, MessagePackPacket, ResponseBodyKey, ResponseCode}
import zio.tarantool.{BaseLayers, TarantoolConfig}

object SocketChannelQueuedWriterSpec extends DefaultRunnableSpec with BaseLayers {

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("SocketChannelQueuedWriter")(
      testM("should send request") {
        val connectionMock = TarantoolConnectionMock.SendRequest(anything, value(Some(1)))
        val configLayer = ZLayer.succeed(TarantoolConfig())
        val socketChannelQueuedWriterLayer =
          (loggingLayer ++ configLayer ++ connectionMock) >>> socketChannelWriterLayer

        val messagePackPacket = MessagePackPacket(
          Map(
            Header.Sync.value -> MpPositiveFixInt(1),
            Header.SchemaId.value -> MpPositiveFixInt(1),
            Header.Code.value -> MpPositiveFixInt(ResponseCode.Success.value)
          ),
          Map(
            ResponseBodyKey.Data.value -> MpPositiveFixInt(1)
          )
        )

        val result = for {
          _ <- SocketChannelQueuedWriter.send(messagePackPacket)
          queue <- SocketChannelQueuedWriter.requestQueue()
          size <- queue.size
        } yield assert(size)(equalTo(0))

        result.provideLayer(socketChannelQueuedWriterLayer)
      },
      testM("should send request to queue") {
        val connectionMock = TarantoolConnectionMock.SendRequest(anything, value(None))
        val configLayer = ZLayer.succeed(TarantoolConfig())
        val socketChannelQueuedWriterLayer =
          (loggingLayer ++ configLayer ++ connectionMock) >>> socketChannelWriterLayer

        val messagePackPacket = MessagePackPacket(
          Map(
            Header.Sync.value -> MpPositiveFixInt(1),
            Header.SchemaId.value -> MpPositiveFixInt(1),
            Header.Code.value -> MpPositiveFixInt(ResponseCode.Success.value)
          ),
          Map(
            ResponseBodyKey.Data.value -> MpPositiveFixInt(1)
          )
        )

        val result = for {
          _ <- SocketChannelQueuedWriter.send(messagePackPacket)
          queue <- SocketChannelQueuedWriter.requestQueue()
          size <- queue.size
        } yield assert(size)(equalTo(1))

        result.provideLayer(socketChannelQueuedWriterLayer)
      },
      testM("should send request to queue and read it using background fiber") {
        val connectionMock =
          TarantoolConnectionMock.SendRequest(anything, value(None)) ++ TarantoolConnectionMock
            .SendRequest(anything, value(Some(1)))
        val configLayer = ZLayer.succeed(TarantoolConfig())
        val socketChannelQueuedWriterLayer =
          (loggingLayer ++ configLayer ++ connectionMock) >>> socketChannelWriterLayer

        val messagePackPacket = MessagePackPacket(
          Map(
            Header.Sync.value -> MpPositiveFixInt(1),
            Header.SchemaId.value -> MpPositiveFixInt(1),
            Header.Code.value -> MpPositiveFixInt(ResponseCode.Success.value)
          ),
          Map(
            ResponseBodyKey.Data.value -> MpPositiveFixInt(1)
          )
        )

        val result = for {
          _ <- SocketChannelQueuedWriter.send(messagePackPacket)
          size <- SocketChannelQueuedWriter.requestQueue().flatMap(_.size)
          fiber <- SocketChannelQueuedWriter.start()
          _ <- clock.sleep(1 second)
          _ <- fiber.interrupt
          empty <- SocketChannelQueuedWriter.requestQueue().flatMap(_.size)
        } yield assert(size)(equalTo(1)) && assert(empty)(equalTo(0))

        result.provideLayer(Clock.live ++ socketChannelQueuedWriterLayer)
      }
    ) @@ sequential @@ timeout(5 seconds)

  private val socketChannelWriterLayer
    : ZLayer[Has[Logger[String]] with TarantoolConnection, Nothing, SocketChannelQueuedWriter] =
    ZLayer.fromManaged(
      ZManaged.make(
        for {
          logger <- ZIO.service[Logger[String]]
          connection <- ZIO.service[TarantoolConnection.Service]
          queue <- Queue.unbounded[ByteBuffer]
        } yield new SocketChannelQueuedWriter.Live(
          logger,
          connection,
          ExecutionContextManager.singleThreaded(),
          queue
        )
      )(_.close().orDie)
    )
}
