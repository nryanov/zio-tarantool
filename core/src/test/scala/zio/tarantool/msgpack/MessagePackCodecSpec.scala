package zio.tarantool.msgpack

import scodec.bits._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.sequential

object MessagePackCodecSpec extends DefaultRunnableSpec {
  private val mpNil = test("mpnil") {
    encodeAndDecode(MpNil)
  }

  private val mpPositiveFixInt = test("mpPositiveFixInt") {
    encodeAndDecode(MpPositiveFixInt(0)) && encodeAndDecode(MpPositiveFixInt(127))
  }

  private val mpNegativeFixInt = test("mpNegativeFixInt") {
    encodeAndDecode(MpNegativeFixInt(-32)) && encodeAndDecode(MpNegativeFixInt(-1))
  }

  private val mpUint8 = test("mpUint8") {
    encodeAndDecode(MpUint8(0)) && encodeAndDecode(MpUint8(255))
  }

  private val mpUint16 = test("mpUint16") {
    encodeAndDecode(MpUint16(0)) && encodeAndDecode(MpUint16(65535))
  }

  private val mpUint32 = test("mpUint32") {
    encodeAndDecode(MpUint32(0)) && encodeAndDecode(MpUint32(4294967295L))
  }

  private val mpUint64 = test("mpUint64") {
    encodeAndDecode(MpUint64(0)) && encodeAndDecode(MpUint64(Long.MaxValue)) // BigInt ?
  }

  private val mpInt8 = test("mpInt8") {
    encodeAndDecode(MpInt8(Byte.MinValue)) && encodeAndDecode(MpInt8(Byte.MaxValue))
  }

  private val mpInt16 = test("mpInt16") {
    encodeAndDecode(MpInt16(Short.MinValue)) && encodeAndDecode(MpInt16(Short.MaxValue))
  }

  private val mpInt32 = test("mpInt32") {
    encodeAndDecode(MpInt32(Int.MinValue)) && encodeAndDecode(MpInt32(Int.MaxValue))
  }

  private val mpInt64 = test("mpInt64") {
    encodeAndDecode(MpInt64(Long.MinValue)) && encodeAndDecode(MpInt64(Long.MaxValue))
  }

  private val mpBoolean = test("mpBoolean") {
    encodeAndDecode(MpTrue) && encodeAndDecode(MpFalse)
  }

  private val mpFloat32 = test("mpFloat32") {
    encodeAndDecode(MpFloat32(Float.MinValue)) && encodeAndDecode(MpFloat32(Float.MaxValue))
  }

  private val mpFloat64 = test("mpFloat64") {
    encodeAndDecode(MpFloat64(Double.MinValue)) && encodeAndDecode(MpFloat64(Double.MaxValue))
  }

  private val mpFixString = test("mpFixString") {
    encodeAndDecode(MpFixString("")) && encodeAndDecode(MpFixString("a" * 10))
  }

  private val mpString8 = test("mpString8") {
    encodeAndDecode(MpString8("")) && encodeAndDecode(MpString8("a" * 255))
  }

  private val mpString16 = test("mpString16") {
    encodeAndDecode(MpString16("")) && encodeAndDecode(MpString16("a" * 65535))
  }

  private val mpString32 = test("mpString32") {
    encodeAndDecode(MpString32("")) && encodeAndDecode(MpString32("a" * 100000))
  }

  private val mpFixMap = test("mpFixMap") {
    encodeAndDecode(
      MpFixMap(
        Map(MpPositiveFixInt(1) -> MpFixString("test"))
      )
    )
  }

  private val mpMap16 = test("mpMap16") {
    encodeAndDecode(
      MpMap16(
        Map(MpPositiveFixInt(1) -> MpFixString("test"))
      )
    )
  }

  private val mpMap32 = test("mpMap32") {
    encodeAndDecode(
      MpMap32(
        Map(MpPositiveFixInt(1) -> MpFixString("test"))
      )
    )
  }

  private val mpFixArray = test("mpFixArray") {
    encodeAndDecode(MpFixArray(Vector(MpPositiveFixInt(1))))
  }

  private val mpArray16 = test("mpArray16") {
    encodeAndDecode(MpArray16(Vector(MpPositiveFixInt(1))))
  }

  private val mpArray32 = test("mpArray32") {
    encodeAndDecode(MpArray32(Vector(MpPositiveFixInt(1))))
  }

  private val mpBinary8 = test("mpBinary8") {
    encodeAndDecode(MpBinary8(ByteVector(1)))
  }

  private val mpBinary16 = test("mpBinary16") {
    encodeAndDecode(MpBinary16(ByteVector(1)))
  }

  private val mpBinary32 = test("mpBinary32") {
    encodeAndDecode(MpBinary32(ByteVector(1)))
  }

  private val mpFixExtension1 = test("mpFixExtension1") {
    encodeAndDecode(MpFixExtension1(1, hex"A"))
  }

  private val mpFixExtension2 = test("mpFixExtension2") {
    encodeAndDecode(MpFixExtension2(2, hex"000A"))
  }

  private val mpFixExtension4 = test("mpFixExtension4") {
    encodeAndDecode(MpFixExtension4(4, hex"0000000A"))
  }

  private val mpFixExtension8 = test("mpFixExtension8") {
    encodeAndDecode(MpFixExtension8(8, hex"000000000000000A"))
  }

  private val mpFixExtension16 = test("mpFixExtension16") {
    encodeAndDecode(MpFixExtension16(16, hex"0000000000000000000000000000000A"))
  }

  private val mpExtension8 = test("mpExtension8") {
    encodeAndDecode(MpExtension8(1, 8, hex"A"))
  }

  private val mpExtension16 = test("mpExtension16") {
    encodeAndDecode(MpExtension16(1, 16, hex"A"))
  }

  private val mpExtension32 = test("mpExtension32") {
    encodeAndDecode(MpExtension32(1, 32, hex"A"))
  }

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("MessagePackCodec")(
      mpPositiveFixInt,
      mpNegativeFixInt,
      mpNil,
      mpUint8,
      mpUint16,
      mpUint32,
      mpUint64,
      mpInt8,
      mpInt16,
      mpInt32,
      mpInt64,
      mpBoolean,
      mpFloat32,
      mpFloat64,
      mpFixString,
      mpString8,
      mpString16,
      mpString32,
      mpFixMap,
      mpMap16,
      mpMap32,
      mpFixArray,
      mpArray16,
      mpArray32,
      mpBinary8,
      mpBinary16,
      mpBinary32,
      mpFixExtension1,
      mpFixExtension2,
      mpFixExtension4,
      mpFixExtension8,
      mpFixExtension16,
      mpExtension8,
      mpExtension16,
      mpExtension32
    ) @@ sequential

  private def encodeAndDecode(mp: MessagePack): TestResult = {
    val bits = MessagePackCodec.encode(mp).require
    val decoded = MessagePackCodec.decode(bits).require

    assert(decoded.remainder)(equalTo(BitVector.empty)) && assert(decoded.value)(equalTo(mp))
  }
}
