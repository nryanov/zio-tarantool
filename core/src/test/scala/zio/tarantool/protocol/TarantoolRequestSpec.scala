package zio.tarantool.protocol

import zio.test._
import zio.test.Assertion._
import zio.test.environment.testEnvironment
import zio.tarantool.msgpack.Encoder

object TarantoolRequestSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TarantoolRequest")(
      testM("should create packet") {
        val expected = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(OperationCode.Ping.value)
          )
        )
        val request = TarantoolRequest(OperationCode.Ping, 1, None, Map.empty)
        val result = TarantoolRequest.createPacket(request)
        assertM(result)(equalTo(expected))
      }
    ).provideCustomLayerShared(testEnvironment)
}
