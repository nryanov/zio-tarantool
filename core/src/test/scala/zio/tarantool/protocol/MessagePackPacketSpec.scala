package zio.tarantool.protocol

import zio.test._
import zio.test.Assertion._
import zio.tarantool.Generators._
import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.{Encoder, MessagePack, MpInt8, MpPositiveFixInt}

import scodec.bits.ByteVector
import zio.tarantool.TarantoolError.UnknownResponseCode
import zio.test.TestAspect.sequential

object MessagePackPacketSpec extends DefaultRunnableSpec {
  private def createPacket(body: MessagePack): MessagePackPacket =
    MessagePackPacket(Map.empty[Long, MessagePack], Map(RequestBodyKey.Key.value -> body))

  private val messagePackPacketProcessingSuite = suite("process message pack packet internals")(
    testM("should extract code") {
      val packet =
        MessagePackPacket(Map(Header.Code.value -> MpPositiveFixInt(ResponseCode.Success.value)))
      val result = MessagePackPacket.extractCode(packet)
      assertM(result)(equalTo(ResponseCode.Success))
    },
    testM("should fail with ProtocolError when trying to extract not existing code") {
      val packet = MessagePackPacket(Map.empty)
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
      val packet = MessagePackPacket(Map(Header.Code.value -> MpInt8(0x8123)))
      val result = MessagePackPacket.extractCode(packet)
      assertM(result)(equalTo(ResponseCode.Error(0x0123)))
    },
    testM("should fail with ProtocolError when trying to extract incorrect error code") {
      val packet = MessagePackPacket(Map(Header.Code.value -> MpInt8(0x1234)))
      val result = MessagePackPacket.extractCode(packet)
      assertM(result.run)(
        fails(
          equalTo(TarantoolError.ProtocolError(s"Code ${0x1234} does not follow 0x8XXX format"))
        )
      )
    },
    testM("should extract data response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, MessagePack],
          Some(Map(ResponseBodyKey.Data.value -> MpPositiveFixInt(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.DataResponse))
    },
    testM("should extract sql response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, MessagePack],
          Some(Map(ResponseBodyKey.SqlInfo.value -> MpPositiveFixInt(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.SqlResponse))
    },
    testM("should extract error24 response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, MessagePack],
          Some(Map(ResponseBodyKey.Error24.value -> MpPositiveFixInt(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.ErrorResponse))
    },
    testM("should extract error response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, MessagePack],
          Some(Map(ResponseBodyKey.Error.value -> MpPositiveFixInt(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.ErrorResponse))
    },
    testM("should extract ping response type") {
      val packet =
        MessagePackPacket(Map.empty[Long, MessagePack], Some(Map.empty[Long, MessagePack]))
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.PingResponse))
    },
    testM("should fail if packet has unknown response type") {
      val packet =
        MessagePackPacket(Map.empty[Long, MessagePack], Some(Map(100500L -> MpPositiveFixInt(1))))
      assertM(MessagePackPacket.responseType(packet).run)(
        fails(equalTo(UnknownResponseCode(packet)))
      )
    },
    testM("should extract data") {
      val packet = MessagePackPacket(
        Map.empty[Long, MessagePack],
        Some(Map(ResponseBodyKey.Data.value -> MpPositiveFixInt(123)))
      )
      assertM(MessagePackPacket.extractData(packet))(equalTo(MpPositiveFixInt(123)))
    },
    testM("should fail with ProtocolError when trying to extract not existing data") {
      val packet =
        MessagePackPacket(Map.empty[Long, MessagePack], Some(Map.empty[Long, MessagePack]))
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
      val packet = MessagePackPacket(Map(Header.Sync.value -> MpPositiveFixInt(1)))
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
      val packet = MessagePackPacket(Map(Header.SchemaId.value -> MpPositiveFixInt(2)))
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

  private val messagePackPacketToBufferSuite = suite("convert packet into byte buffer")(
    testM("convert byte") {
      checkM(byte()) { value =>
        val encoded = Encoder[Byte].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert short") {
      checkM(short()) { value =>
        val encoded = Encoder[Short].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert int") {
      checkM(int()) { value =>
        val encoded = Encoder[Int].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert long") {
      checkM(long()) { value =>
        val encoded = Encoder[Long].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert float") {
      checkM(float()) { value =>
        val encoded = Encoder[Float].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert double") {
      checkM(double()) { value =>
        val encoded = Encoder[Double].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert string") {
      checkM(nonEmptyString(64)) { value =>
        val encoded = Encoder[String].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert boolean") {
      checkM(bool()) { value =>
        val encoded = Encoder[Boolean].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert binary") {
      checkM(listOf(10, byte())) { value =>
        val encoded = Encoder[ByteVector].encodeUnsafe(ByteVector(value))
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert vector") {
      checkM(listOf(32, nonEmptyString(64))) { value =>
        val encoded = Encoder[Vector[String]].encodeUnsafe(value.toVector)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert map") {
      checkM(mapOf(32, int(), int())) { value =>
        val encoded = Encoder[Map[Int, Int]].encodeUnsafe(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    }
  )

  private def successfullyConvertPacketToBuffer(mp: MessagePack) =
    for {
      _ <- MessagePackPacket.toBuffer(createPacket(mp))
    } yield assertCompletes

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("MessagePackPacket")(
      messagePackPacketProcessingSuite,
      messagePackPacketToBufferSuite
    ) @@ sequential
}
