package zio.tarantool.protocol

import org.msgpack.value.impl.ImmutableLongValueImpl
import zio.test._
import zio.test.Assertion._

object TarantoolRequestSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TarantoolRequest")(
      testM("should create packet") {
        val expected = MessagePackPacket(
          Map(
            Header.Sync.value -> new ImmutableLongValueImpl(1),
            Header.Code.value -> new ImmutableLongValueImpl(RequestCode.Ping.value.toLong)
          )
        )
        val request = TarantoolRequest(RequestCode.Ping, 1, Map.empty)
        val result = TarantoolRequest.createPacket(request)
        assertM(result)(equalTo(expected))
      }
    )
}
