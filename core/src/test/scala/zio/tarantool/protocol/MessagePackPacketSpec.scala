package zio.tarantool.protocol

import zio.test._
import zio.test.Assertion._
import zio.tarantool.Generators._
import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.{Encoder, MessagePack, MpPositiveFixInt}
import java.nio.ByteBuffer

import scodec.bits.ByteVector
import zio.test.TestAspect.sequential

object MessagePackPacketSpec extends DefaultRunnableSpec {
  private def createPacket(body: MessagePack): MessagePackPacket =
    MessagePackPacket(Map.empty, Map(RequestBodyKey.Key.value -> body))

  private val messagePackPacketProcessingSuite = suite("process message pack packet internals")(
    testM("should convert to ByteBuffer") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(RequestCode.Ping.value)
        )
      )
      val result = MessagePackPacket.toBuffer(packet)
      assertM(result)(isSubtype[ByteBuffer](anything))
    },
    testM("should extract code") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(
            ResponseCode.Success.value
          )
        )
      )
      val result = MessagePackPacket.extractCode(packet)
      assertM(result)(equalTo(ResponseCode.Success))
    },
    testM("should fail with ProtocolError when trying to extract not existing code") {
      val packet =
        MessagePackPacket(Map(Header.Sync.value -> Encoder[Long].encodeUnsafe(1)))
      val result = MessagePackPacket.extractCode(packet)
      assertM(result.run)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(s"Packet has no Code in header (${packet.header})")
          )
        )
      )
    },
    testM("should extract error code") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(0x8123)
        )
      )
      val result = MessagePackPacket.extractCode(packet)
      assertM(result)(equalTo(ResponseCode.Error(0x0123)))
    },
    testM("should fail with ProtocolError when trying to extract incorrect error code") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(0x1234)
        )
      )
      val result = MessagePackPacket.extractCode(packet)
      assertM(result.run)(
        fails(
          equalTo(TarantoolError.ProtocolError(s"Code ${0x1234} does not follow 0x8XXX format"))
        )
      )
    },
    testM("should extract data") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(0x0)
        ),
        Some(
          Map(
            ResponseBodyKey.Data.value -> Encoder[Long].encodeUnsafe(123)
          )
        )
      )
      val result = MessagePackPacket.extractData(packet)
      assertM(result)(equalTo(MpPositiveFixInt(123)))
    },
    testM("should fail with ProtocolError when trying to extract not existing data") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(0x0)
        ),
        Some(Map.empty[Long, MessagePack])
      )
      val result = MessagePackPacket.extractData(packet)
      assertM(result.run)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(
              s"Packet has no ${ResponseBodyKey.Data} value in body part ${packet.body}"
            )
          )
        )
      )
    },
    testM("should extract syncId") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(0x8123)
        )
      )
      val result = MessagePackPacket.extractSyncId(packet)
      assertM(result)(equalTo(1.toLong))
    },
    testM("should fail with ProtocolError when trying to extract not existing syncId") {
      val packet = MessagePackPacket(Map.empty[Long, MessagePack])
      val result = MessagePackPacket.extractSyncId(packet)
      assertM(result.run)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(s"Packet has no SyncId in header (${packet.header})")
          )
        )
      )
    },
    testM("should extract schemaId") {
      val packet = MessagePackPacket(
        Map(
          Header.Sync.value -> Encoder[Long].encodeUnsafe(1),
          Header.Code.value -> Encoder[Long].encodeUnsafe(0x8123),
          Header.SchemaId.value -> Encoder[Long].encodeUnsafe(2)
        )
      )
      val result = MessagePackPacket.extractSchemaId(packet)
      assertM(result)(equalTo(2.toLong))
    },
    testM("should fail with ProtocolError when trying to extract not existing schemaId") {
      val packet = MessagePackPacket(Map.empty[Long, MessagePack])
      val result = MessagePackPacket.extractSchemaId(packet)
      assertM(result.run)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(s"Packet has no SchemaId in header (${packet.header})")
          )
        )
      )
    }
  )

  private val messagePAckPacketEncodingSuite = suite("encode packet")(
    testM("encode/decode byte") {
      checkM(byte()) { value =>
        val encoded =
          Encoder[Byte].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode short") {
      checkM(short()) { value =>
        val encoded =
          Encoder[Short].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode int") {
      checkM(int()) { value =>
        val encoded =
          Encoder[Int].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode long") {
      checkM(long()) { value =>
        val encoded =
          Encoder[Long].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode float") {
      checkM(float()) { value =>
        val encoded =
          Encoder[Float].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode double") {
      checkM(double()) { value =>
        val encoded =
          Encoder[Double].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode string") {
      checkM(nonEmptyString(64)) { value =>
        val encoded =
          Encoder[String].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode boolean") {
      checkM(bool()) { value =>
        val encoded =
          Encoder[Boolean].encode(value).getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode binary") {
      checkM(listOf(10, byte())) { value =>
        val encoded =
          Encoder[ByteVector]
            .encode(ByteVector(value))
            .getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode vector") {
      checkM(listOf(32, nonEmptyString(64))) { value =>
        val encoded =
          Encoder[Vector[String]]
            .encode(value.toVector)
            .getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    },
    testM("encode/decode map") {
      checkM(mapOf(32, int(), int())) { value =>
        val encoded =
          Encoder[Map[Int, Int]]
            .encode(value)
            .getOrElse(throw new RuntimeException("Fail to encode"))

        for {
          _ <- MessagePackPacket.toBuffer(createPacket(encoded))
        } yield assertCompletes
      }
    }
  )

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("MessagePackPacket")(
      messagePackPacketProcessingSuite,
      messagePAckPacketEncodingSuite
    ) @@ sequential
}
