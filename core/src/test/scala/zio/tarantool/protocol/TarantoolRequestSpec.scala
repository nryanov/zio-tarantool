package zio.tarantool.protocol

import org.msgpack.value.impl.ImmutableLongValueImpl
import _root_.zio.test._
import _root_.zio.test.Assertion._

object TarantoolRequestSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] =
    suite("TarantoolRequest")(
      test("should create packet") {
        val expected = MessagePackPacket(
          Map(
            Header.Sync.value -> new ImmutableLongValueImpl(1),
            Header.Code.value -> new ImmutableLongValueImpl(RequestCode.Ping.value.toLong)
          )
        )
        val request = TarantoolRequest(RequestCode.Ping, 1, Map.empty)
        val result = TarantoolRequest.createPacket(request)
        assertZIO(result)(equalTo(expected))
      }
    )
}
