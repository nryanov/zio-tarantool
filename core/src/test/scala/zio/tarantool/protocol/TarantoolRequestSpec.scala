//package zio.tarantool.protocol
// fixme
//import zio.test._
//import zio.test.Assertion._
//import zio.tarantool.msgpack.MpPositiveFixInt
//
//object TarantoolRequestSpec extends DefaultRunnableSpec {
//  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
//    suite("TarantoolRequest")(
//      testM("should create packet") {
//        val expected = MessagePackPacket(
//          Map(
//            Header.Sync.value -> MpPositiveFixInt(1),
//            Header.Code.value -> MpPositiveFixInt(RequestCode.Ping.value)
//          )
//        )
//        val request = TarantoolRequest(RequestCode.Ping, 1, Map.empty)
//        val result = TarantoolRequest.createPacket(request)
//        assertM(result)(equalTo(expected))
//      }
//    )
//}
