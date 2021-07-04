package zio.tarantool.core

import zio._
import zio.test._
import zio.stream.ZStream
import zio.test.Assertion._
import zio.test.TestAspect.sequential
import zio.tarantool.core.ByteStream.decoder
import zio.tarantool.codec.MessagePackPacketCodec
import zio.tarantool.msgpack.{MessagePackCodec, MpUint32}
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
          encodedPacket <- ZIO.effect(MessagePackPacketCodec.encode(packet).require)
          size = MpUint32(encodedPacket.bytes.length)
          encodedSize <- ZIO.effect(MessagePackCodec.encode(size).require)
          result <- ZStream
            .fromIterable(encodedSize.++(encodedPacket).toByteArray)
            .transduce(decoder)
            .runHead
        } yield assert(result)(isSome(equalTo(packet)))
      }
    ) @@ sequential
}
