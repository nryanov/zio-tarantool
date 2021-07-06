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
            Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Header.Code.value -> Encoder[Long].encodeUnsafe(RequestCode.Ping.value)
          )
        )
        val request = TarantoolRequest(RequestCode.Ping, 1, Map.empty)
        val result = TarantoolRequest.createPacket(request)
        assertM(result)(equalTo(expected))
      }
    )
}
