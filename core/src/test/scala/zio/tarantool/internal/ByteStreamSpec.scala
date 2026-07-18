package zio.tarantool.internal

import java.nio.ByteBuffer

import _root_.zio.test._
import _root_.zio.stream.ZStream
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect.sequential
import zio.tarantool.internal.ByteStream.decoder
import zio.tarantool.codec.MessagePackPacketSerDe
import zio.tarantool.protocol.{RequestCode, TarantoolRequest}

object ByteStreamSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] =
    suite("ByteStream")(
      test("read empty chunk and return none") {
        for {
          result <- ZStream().transduce(decoder).runHead
        } yield assert(result)(isNone)
      },
      test("read non empty chunk and return MessagePackPacket") {
        for {
          packet <- TarantoolRequest.createPacket(
            TarantoolRequest(RequestCode.Ping, 1, Map.empty)
          )
          encodedPacket <- MessagePackPacketSerDe.serialize(packet)
          size = ByteBuffer.allocate(5).put(0xd2.toByte).putInt(encodedPacket.length)
          result <- ZStream.fromIterable(size.array() ++ encodedPacket).transduce(decoder).runHead
        } yield assert(result)(isSome(equalTo(packet)))
      }
    ) @@ sequential
}
