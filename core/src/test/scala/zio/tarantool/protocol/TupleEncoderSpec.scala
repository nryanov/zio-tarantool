package zio.tarantool.protocol

import org.scalatest.OptionValues
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import scodec.bits.ByteVector
import zio.tarantool.BaseSpec
import zio.tarantool.msgpack.MpFixArray
import zio.tarantool.Generators._

class TupleEncoderSpec extends BaseSpec with ScalaCheckPropertyChecks with OptionValues {
  "TupleEncoder for primitive types" should {
    "encode/decode unit" in {
      val tupleEncoder: TupleEncoder[Unit] = TupleEncoder[Unit]

      val encoded = tupleEncoder.encode(()).toOption.value
      val decoded: Unit = tupleEncoder.decode(encoded, 0).toOption.value

      encoded shouldBe MpFixArray(Vector.empty)
      decoded shouldBe (())
    }

    "encode/decode byte" in {
      forAll(byte()) { value =>
        val tupleEncoder: TupleEncoder[Byte] = TupleEncoder[Byte]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode short" in {
      forAll(short()) { value =>
        val tupleEncoder: TupleEncoder[Short] = TupleEncoder[Short]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode int" in {
      forAll(int()) { value =>
        val tupleEncoder: TupleEncoder[Int] = TupleEncoder[Int]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode long" in {
      forAll(long()) { value =>
        val tupleEncoder: TupleEncoder[Long] = TupleEncoder[Long]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode float" in {
      forAll(float()) { value =>
        val tupleEncoder: TupleEncoder[Float] = TupleEncoder[Float]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode double" in {
      forAll(double()) { value =>
        val tupleEncoder: TupleEncoder[Double] = TupleEncoder[Double]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode string" in {
      forAll(nonEmptyString(32)) { value =>
        val tupleEncoder: TupleEncoder[String] = TupleEncoder[String]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode map" in {
      forAll(mapOf(32, long(), int())) { value =>
        val tupleEncoder: TupleEncoder[Map[Long, Int]] = TupleEncoder[Map[Long, Int]]
        checkTupleEncoder(value, tupleEncoder)
      }
    }

    "encode/decode byte array" in {
      forAll(nonEmptyListOf(32, byte())) { value =>
        val tupleEncoder: TupleEncoder[ByteVector] = TupleEncoder[ByteVector]
        checkTupleEncoder(ByteVector(value), tupleEncoder)
      }
    }

    "encode/decode vector" in {
      forAll(nonEmptyListOf(32, int())) { value =>
        val tupleEncoder: TupleEncoder[Vector[Int]] = TupleEncoder[Vector[Int]]
        checkTupleEncoder(value.toVector, tupleEncoder)
      }
    }
  }

  private def checkTupleEncoder[A](value: A, tupleEncoder: TupleEncoder[A]) = {
    val encoded = tupleEncoder.encode(value).toOption.value
    val decoded = tupleEncoder.decode(encoded, 0).toOption.value

    encoded.value.size shouldBe 1
    decoded shouldBe value
  }
}
