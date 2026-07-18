package zio.tarantool.codec

import java.util.UUID

import org.msgpack.value.impl._
import zio.ZIO
import _root_.zio.test._
import _root_.zio.test.Assertion._
import zio.tarantool.Generators._
import zio.tarantool.TarantoolError
import _root_.zio.test.ZIOSpecDefault
import zio.tarantool.protocol.Implicits._

object TupleEncoderSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment, Any] =
    suite("TupleEncoder for primitive types")(
      test("encode/decode Unit") {
        val tupleEncoder: TupleEncoder[Unit] = TupleEncoder[Unit]

        for {
          encoded <- tupleEncoder.encodeM(())
          decoded <- tupleEncoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(new ImmutableArrayValueImpl(Array(ImmutableNilValueImpl.get())))
        ) && assert(decoded)(isUnit)
      },
      test("encode/decode Byte") {
        check(byte()) { value =>
          val tupleEncoder: TupleEncoder[Byte] = TupleEncoder[Byte]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Short") {
        check(short()) { value =>
          val tupleEncoder: TupleEncoder[Short] = TupleEncoder[Short]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Int") {
        check(int()) { value =>
          val tupleEncoder: TupleEncoder[Int] = TupleEncoder[Int]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Long") {
        check(long()) { value =>
          val tupleEncoder: TupleEncoder[Long] = TupleEncoder[Long]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Float") {
        check(float()) { value =>
          val tupleEncoder: TupleEncoder[Float] = TupleEncoder[Float]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Double") {
        check(double()) { value =>
          val tupleEncoder: TupleEncoder[Double] = TupleEncoder[Double]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Boolean") {
        check(bool()) { value =>
          val tupleEncoder: TupleEncoder[Boolean] = TupleEncoder[Boolean]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode String") {
        check(nonEmptyString(64)) { value =>
          val tupleEncoder: TupleEncoder[String] = TupleEncoder[String]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode UUID") {
        check(uuid()) { value =>
          val tupleEncoder: TupleEncoder[UUID] = TupleEncoder[UUID]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode BigInt") {
        check(bigInt()) { value =>
          val tupleEncoder: TupleEncoder[BigInt] = TupleEncoder[BigInt]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode BigDecimal") {
        check(bigDecimal()) { value =>
          val tupleEncoder: TupleEncoder[BigDecimal] = TupleEncoder[BigDecimal]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Map") {
        check(mapOf(32, long(), int())) { value =>
          val tupleEncoder: TupleEncoder[Map[Long, Int]] = TupleEncoder[Map[Long, Int]]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      test("encode/decode Array[Byte]") {
        check(nonEmptyListOf(32, byte())) { value =>
          val tupleEncoder: TupleEncoder[Array[Byte]] = TupleEncoder[Array[Byte]]
          for {
            encoded <- tupleEncoder.encodeM(value.toArray)
            decoded <- tupleEncoder.decodeM(encoded)
          } yield assert(encoded.isArrayValue)(isTrue) && assert(decoded.toList)(equalTo(value))
        }
      },
      test("encode/decode Vector") {
        check(nonEmptyListOf(32, int())) { value =>
          val tupleEncoder: TupleEncoder[Vector[Int]] = TupleEncoder[Vector[Int]]
          checkTupleEncoder(value.toVector, tupleEncoder)
        }
      }
    )

  private def checkTupleEncoder[A](
    value: A,
    tupleEncoder: TupleEncoder[A]
  ): ZIO[Any, TarantoolError.CodecError, TestResult] =
    for {
      encoded <- tupleEncoder.encodeM(value)
      decoded <- tupleEncoder.decodeM(encoded)
    } yield assert(decoded)(equalTo(value))
}
