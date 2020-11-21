package zio.tarantool.msgpack

import org.scalatest.OptionValues
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector
import zio.tarantool.BaseSpec
import zio.tarantool.Generators._

class EncoderSpec extends BaseSpec with ScalaCheckPropertyChecks with OptionValues {
  "Encoder" should {
    "encode/decode byte" in {
      forAll(byte()) { value =>
        val encoded = Encoder[Byte].encode(value).toOption.value
        val decoded = Encoder[Byte].decode(encoded).toOption.value

        if (value < -(1 << 5)) {
          encoded shouldBe MpInt8(value)
        } else {
          if (value < 0) encoded shouldBe MpNegativeFixInt(value)
          else encoded shouldBe MpPositiveFixInt(value)
        }

        decoded shouldBe value
      }
    }

    "encode/decode short" in {
      forAll(short()) { value =>
        val encoded = Encoder[Short].encode(value).toOption.value
        val decoded = Encoder[Short].decode(encoded).toOption.value

        if (value < -(1 << 5)) {
          if (value < -(1 << 7)) {
            encoded shouldBe MpInt16(value)
          } else {
            encoded shouldBe MpInt8(value)
          }
        } else if (value < (1 << 7)) {
          if (value < 0) encoded shouldBe MpNegativeFixInt(value)
          else encoded shouldBe MpPositiveFixInt(value)
        } else {
          if (value < (1 << 8)) {
            encoded shouldBe MpUint8(value)
          } else {
            encoded shouldBe MpUint16(value)
          }
        }

        decoded shouldBe value
      }
    }

    "encode/decode int" in {
      forAll(int()) { value =>
        val encoded = Encoder[Int].encode(value).toOption.value
        val decoded = Encoder[Int].decode(encoded).toOption.value

        if (value < -(1 << 5)) {
          if (value < -(1 << 15)) encoded shouldBe MpInt32(value)
          else if (value < -(1 << 7)) encoded shouldBe MpInt16(value)
          else encoded shouldBe MpInt8(value)
        } else if (value < (1 << 7)) {
          if (value >= 0) encoded shouldBe MpPositiveFixInt(value)
          else encoded shouldBe MpNegativeFixInt(value)
        } else {
          if (value < (1 << 8)) encoded shouldBe MpUint8(value)
          else if (value < (1 << 16)) encoded shouldBe MpUint16(value)
          else encoded shouldBe MpUint32(value)
        }

        decoded shouldBe value
      }
    }

    "encode/decode long" in {
      forAll(long()) { value =>
        val encoded = Encoder[Long].encode(value).toOption.value
        val decoded = Encoder[Long].decode(encoded).toOption.value

        if (value < -(1L << 5)) {
          if (value < -(1L << 15)) {
            if (value < -(1L << 31)) encoded shouldBe MpInt64(value)
            else encoded shouldBe MpInt32(value.toInt)
          } else {
            if (value < -(1 << 7)) encoded shouldBe MpInt16(value.toInt)
            else encoded shouldBe MpInt8(value.toInt)
          }
        } else if (value < (1 << 7)) {
          if (value >= 0) encoded shouldBe MpPositiveFixInt(value.toInt)
          else encoded shouldBe MpNegativeFixInt(value.toInt)
        } else {
          if (value < (1L << 16)) {
            if (value < (1 << 8)) encoded shouldBe MpUint8(value.toInt)
            else encoded shouldBe MpUint16(value.toInt)
          } else {
            if (value < (1L << 32)) encoded shouldBe MpUint32(value)
            else encoded shouldBe MpUint64(value)
          }
        }

        decoded shouldBe value
      }
    }

    "encode/decode float" in {
      forAll(float()) { value =>
        val encoded = Encoder[Float].encode(value).toOption.value
        val decoded = Encoder[Float].decode(encoded).toOption.value

        encoded shouldBe MpFloat32(value)
        decoded shouldBe value
      }
    }

    "encode/decode double" in {
      forAll(double()) { value =>
        val encoded = Encoder[Double].encode(value).toOption.value
        val decoded = Encoder[Double].decode(encoded).toOption.value

        encoded shouldBe MpFloat64(value)
        decoded shouldBe value
      }
    }

    "encode/decode string" in {
      forAll(nonEmptyString(64)) { value =>
        val encoded = Encoder[String].encode(value).toOption.value
        val decoded = Encoder[String].decode(encoded).toOption.value

        val len = value.length
        if (len < (1 << 5)) {
          encoded shouldBe MpFixString(value)
        } else if (len < (1 << 8)) {
          encoded shouldBe MpString8(value)
        } else if (len < (1 << 16)) {
          encoded shouldBe MpString16(value)
        } else {
          encoded shouldBe MpString32(value)
        }

        decoded shouldBe value
      }
    }

    "encode/decode boolean" in {
      forAll(bool()) { value =>
        val encoded = Encoder[Boolean].encode(value).toOption.value
        val decoded = Encoder[Boolean].decode(encoded).toOption.value

        if (value) encoded shouldBe MpTrue
        else encoded shouldBe MpFalse
        decoded shouldBe value
      }
    }

    "encode/decode binary" in {
      forAll(listOf(10, byte())) { value =>
        val encoded = Encoder[ByteVector].encode(ByteVector(value)).toOption.value
        val decoded = Encoder[ByteVector].decode(encoded).toOption.value

        // check only small vectors
        encoded shouldBe MpBinary8(ByteVector(value))
        decoded shouldBe ByteVector(value)
      }
    }

    "encode/decode vector" in {
      forAll(listOf(32, int())) { value =>
        val encoded = Encoder[Vector[Int]].encode(value.toVector).toOption.value
        val decoded = Encoder[Vector[Int]].decode(encoded).toOption.value

        if (value.length <= 15) encoded shouldBe a[MpFixArray]
        else encoded shouldBe a[MpArray16]
        decoded shouldBe value.toVector
      }
    }

    "encode/decode map" in {
      forAll(mapOf(32, int(), int())) { value =>
        val encoded = Encoder[Map[Int, Int]].encode(value).toOption.value
        val decoded = Encoder[Map[Int, Int]].decode(encoded).toOption.value

        if (value.size <= 15) encoded shouldBe a[MpFixMap]
        else encoded shouldBe a[MpMap16]
        decoded shouldBe value
      }
    }
  }
}
