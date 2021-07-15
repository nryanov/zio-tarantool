package zio.tarantool.protocol

import java.util.UUID

import org.msgpack.value.Value
import org.msgpack.value.impl._
import zio.test._
import zio.test.Assertion._
import zio.tarantool.Generators._
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.UnknownResponseCode
import zio.tarantool.codec.Encoder
import zio.test.TestAspect.sequential

object MessagePackPacketSpec extends DefaultRunnableSpec {
  private def createPacket(body: Value): MessagePackPacket =
    MessagePackPacket(Map.empty[Long, Value], Map(RequestBodyKey.Key.value -> body))

  private val messagePackPacketProcessingSuite = suite("process message pack packet internals")(
    testM("should extract code") {
      val packet =
        MessagePackPacket(
          Map(Header.Code.value -> new ImmutableLongValueImpl(ResponseCode.Success.value))
        )
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
      val packet = MessagePackPacket(Map(Header.Code.value -> new ImmutableLongValueImpl(0x8123)))
      val result = MessagePackPacket.extractCode(packet)
      assertM(result)(equalTo(ResponseCode.Error(0x0123)))
    },
    testM("should fail with ProtocolError when trying to extract incorrect error code") {
      val packet = MessagePackPacket(Map(Header.Code.value -> new ImmutableLongValueImpl(0x1234)))
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
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.Data.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.DataResponse))
    },
    testM("should extract sql response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.SqlInfo.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.SqlResponse))
    },
    testM("should extract error24 response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.Error24.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.ErrorResponse))
    },
    testM("should extract error response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.Error.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.ErrorResponse))
    },
    testM("should extract ping response type") {
      val packet =
        MessagePackPacket(Map.empty[Long, Value], Some(Map.empty[Long, Value]))
      val result = MessagePackPacket.responseType(packet)
      assertM(result)(equalTo(ResponseType.PingResponse))
    },
    testM("should fail if packet has unknown response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(100500L -> new ImmutableLongValueImpl(1)))
        )
      assertM(MessagePackPacket.responseType(packet).run)(
        fails(equalTo(UnknownResponseCode(packet)))
      )
    },
    testM("should extract data") {
      val packet = MessagePackPacket(
        Map.empty[Long, Value],
        Some(Map(ResponseBodyKey.Data.value -> new ImmutableLongValueImpl(123)))
      )
      assertM(MessagePackPacket.extractData(packet))(equalTo(new ImmutableLongValueImpl(123)))
    },
    testM("should fail with ProtocolError when trying to extract not existing data") {
      val packet =
        MessagePackPacket(Map.empty[Long, Value], Some(Map.empty[Long, Value]))
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
      val packet = MessagePackPacket(Map(Header.Sync.value -> new ImmutableLongValueImpl(1)))
      val result = MessagePackPacket.extractSyncId(packet)
      assertM(result)(equalTo(1.toLong))
    },
    testM("should fail with ProtocolError when trying to extract not existing syncId") {
      val packet = MessagePackPacket(Map.empty[Long, Value])
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
      val packet = MessagePackPacket(Map(Header.SchemaId.value -> new ImmutableLongValueImpl(2)))
      val result = MessagePackPacket.extractSchemaId(packet)
      assertM(result)(equalTo(2.toLong))
    },
    testM("should fail with ProtocolError when trying to extract not existing schemaId") {
      val packet = MessagePackPacket(Map.empty[Long, Value])
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
    testM("convert Byte") {
      checkM(byte()) { value =>
        val encoded = Encoder[Byte].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Short") {
      checkM(short()) { value =>
        val encoded = Encoder[Short].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Int") {
      checkM(int()) { value =>
        val encoded = Encoder[Int].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Long") {
      checkM(long()) { value =>
        val encoded = Encoder[Long].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Float") {
      checkM(float()) { value =>
        val encoded = Encoder[Float].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Double") {
      checkM(double()) { value =>
        val encoded = Encoder[Double].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert string") {
      checkM(nonEmptyString(64)) { value =>
        val encoded = Encoder[String].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Boolean") {
      checkM(bool()) { value =>
        val encoded = Encoder[Boolean].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert UUID") {
      checkM(uuid()) { value =>
        val encoded = Encoder[UUID].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert BigInt") {
      checkM(bigInt()) { value =>
        val encoded = Encoder[BigInt].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert BigDecimal") {
      checkM(bigDecimal()) { value =>
        val encoded = Encoder[BigDecimal].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Array[Byte]") {
      checkM(listOf(10, byte())) { value =>
        val encoded = Encoder[Array[Byte]].encode(value.toArray)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Vector") {
      checkM(listOf(32, nonEmptyString(64))) { value =>
        val encoded = Encoder[Vector[String]].encode(value.toVector)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    testM("convert Map") {
      checkM(mapOf(32, int(), int())) { value =>
        val encoded = Encoder[Map[Int, Int]].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    }
  )

  private def successfullyConvertPacketToBuffer(mp: Value) =
    for {
      _ <- MessagePackPacket.toBuffer(createPacket(mp))
    } yield assertCompletes

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("MessagePackPacket")(
      messagePackPacketProcessingSuite,
      messagePackPacketToBufferSuite
    ) @@ sequential
}
