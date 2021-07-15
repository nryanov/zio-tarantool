package zio.tarantool.internal

import java.nio.ByteBuffer

import zio.test._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect.sequential
import zio.tarantool.internal.ByteStream.decoder
import zio.tarantool.codec.MessagePackPacketSerDe
import zio.tarantool.protocol.{RequestCode, TarantoolRequest}

object ByteStreamSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("ByteStream")(
      testM("read empty chunk and return none") {
        for {
          result <- ZStream().transduce(decoder).runHead
        } yield assert(result)(isNone)
      },
      testM("read non empty chunk and return MessagePackPacket") {
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
