package zio.tarantool.protocol

import java.util.UUID

import org.msgpack.value.Value
import org.msgpack.value.impl._
import _root_.zio.test._
import _root_.zio.test.Assertion._
import zio.tarantool.Generators._
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.UnknownResponseCode
import zio.tarantool.codec.Encoder
import _root_.zio.test.TestAspect.sequential

object MessagePackPacketSpec extends ZIOSpecDefault {
  private def createPacket(body: Value): MessagePackPacket =
    MessagePackPacket(Map.empty[Long, Value], Map(RequestBodyKey.Key.value -> body))

  private val messagePackPacketProcessingSuite = suite("process message pack packet internals")(
    test("should extract code") {
      val packet =
        MessagePackPacket(
          Map(Header.Code.value -> new ImmutableLongValueImpl(ResponseCode.Success.value.toLong))
        )
      val result = MessagePackPacket.extractCode(packet)
      assertZIO(result)(equalTo(ResponseCode.Success))
    },
    test("should fail with ProtocolError when trying to extract not existing code") {
      val packet = MessagePackPacket(Map.empty)
      val result = MessagePackPacket.extractCode(packet)
      assertZIO(result.exit)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(s"Packet has no Code in header (${packet.header})")
          )
        )
      )
    },
    test("should extract error code") {
      val packet = MessagePackPacket(Map(Header.Code.value -> new ImmutableLongValueImpl(0x8123)))
      val result = MessagePackPacket.extractCode(packet)
      assertZIO(result)(equalTo(ResponseCode.Error(0x0123)))
    },
    test("should fail with ProtocolError when trying to extract incorrect error code") {
      val packet = MessagePackPacket(Map(Header.Code.value -> new ImmutableLongValueImpl(0x1234)))
      val result = MessagePackPacket.extractCode(packet)
      assertZIO(result.exit)(
        fails(
          equalTo(TarantoolError.ProtocolError(s"Code ${0x1234} does not follow 0x8XXX format"))
        )
      )
    },
    test("should extract data response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.Data.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertZIO(result)(equalTo(ResponseType.DataResponse))
    },
    test("should extract sql response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.SqlInfo.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertZIO(result)(equalTo(ResponseType.SqlResponse))
    },
    test("should extract error24 response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.Error24.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertZIO(result)(equalTo(ResponseType.ErrorResponse))
    },
    test("should extract error response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(ResponseBodyKey.Error.value -> new ImmutableLongValueImpl(0)))
        )
      val result = MessagePackPacket.responseType(packet)
      assertZIO(result)(equalTo(ResponseType.ErrorResponse))
    },
    test("should extract ping response type") {
      val packet =
        MessagePackPacket(Map.empty[Long, Value], Some(Map.empty[Long, Value]))
      val result = MessagePackPacket.responseType(packet)
      assertZIO(result)(equalTo(ResponseType.PingResponse))
    },
    test("should fail if packet has unknown response type") {
      val packet =
        MessagePackPacket(
          Map.empty[Long, Value],
          Some(Map(100500L -> new ImmutableLongValueImpl(1)))
        )
      assertZIO(MessagePackPacket.responseType(packet).exit)(
        fails(equalTo(UnknownResponseCode(packet)))
      )
    },
    test("should extract data") {
      val packet = MessagePackPacket(
        Map.empty[Long, Value],
        Some(Map(ResponseBodyKey.Data.value -> new ImmutableLongValueImpl(123)))
      )
      assertZIO(MessagePackPacket.extractData(packet))(equalTo(new ImmutableLongValueImpl(123)))
    },
    test("should fail with ProtocolError when trying to extract not existing data") {
      val packet =
        MessagePackPacket(Map.empty[Long, Value], Some(Map.empty[Long, Value]))
      val result = MessagePackPacket.extractData(packet)
      assertZIO(result.exit)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(
              s"Packet has no ${ResponseBodyKey.Data} value in body part ${packet.body}"
            )
          )
        )
      )
    },
    test("should extract syncId") {
      val packet = MessagePackPacket(Map(Header.Sync.value -> new ImmutableLongValueImpl(1)))
      val result = MessagePackPacket.extractSyncId(packet)
      assertZIO(result)(equalTo(1.toLong))
    },
    test("should fail with ProtocolError when trying to extract not existing syncId") {
      val packet = MessagePackPacket(Map.empty[Long, Value])
      val result = MessagePackPacket.extractSyncId(packet)
      assertZIO(result.exit)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(s"Packet has no SyncId in header (${packet.header})")
          )
        )
      )
    },
    test("should extract schemaId") {
      val packet = MessagePackPacket(Map(Header.SchemaId.value -> new ImmutableLongValueImpl(2)))
      val result = MessagePackPacket.extractSchemaId(packet)
      assertZIO(result)(equalTo(2.toLong))
    },
    test("should fail with ProtocolError when trying to extract not existing schemaId") {
      val packet = MessagePackPacket(Map.empty[Long, Value])
      val result = MessagePackPacket.extractSchemaId(packet)
      assertZIO(result.exit)(
        fails(
          equalTo(
            TarantoolError.ProtocolError(s"Packet has no SchemaId in header (${packet.header})")
          )
        )
      )
    }
  )

  private val messagePackPacketToBufferSuite = suite("convert packet into byte buffer")(
    test("convert Byte") {
      check(byte()) { value =>
        val encoded = Encoder[Byte].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Short") {
      check(short()) { value =>
        val encoded = Encoder[Short].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Int") {
      check(int()) { value =>
        val encoded = Encoder[Int].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Long") {
      check(long()) { value =>
        val encoded = Encoder[Long].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Float") {
      check(float()) { value =>
        val encoded = Encoder[Float].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Double") {
      check(double()) { value =>
        val encoded = Encoder[Double].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert string") {
      check(nonEmptyString(64)) { value =>
        val encoded = Encoder[String].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Boolean") {
      check(bool()) { value =>
        val encoded = Encoder[Boolean].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert UUID") {
      check(uuid()) { value =>
        val encoded = Encoder[UUID].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert BigInt") {
      check(bigInt()) { value =>
        val encoded = Encoder[BigInt].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert BigDecimal") {
      check(bigDecimal()) { value =>
        val encoded = Encoder[BigDecimal].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Array[Byte]") {
      check(listOf(10, byte())) { value =>
        val encoded = Encoder[Array[Byte]].encode(value.toArray)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Vector") {
      check(listOf(32, nonEmptyString(64))) { value =>
        val encoded = Encoder[Vector[String]].encode(value.toVector)
        successfullyConvertPacketToBuffer(encoded)
      }
    },
    test("convert Map") {
      check(mapOf(32, int(), int())) { value =>
        val encoded = Encoder[Map[Int, Int]].encode(value)
        successfullyConvertPacketToBuffer(encoded)
      }
    }
  )

  private def successfullyConvertPacketToBuffer(mp: Value) =
    for {
      _ <- MessagePackPacket.toBuffer(createPacket(mp))
    } yield assertCompletes

  override def spec: Spec[TestEnvironment, Any] =
    suite("MessagePackPacket")(
      messagePackPacketProcessingSuite,
      messagePackPacketToBufferSuite
    ) @@ sequential
}
