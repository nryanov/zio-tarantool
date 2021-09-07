package zio.tarantool.codec

import java.util.UUID

import org.msgpack.value.impl._
import zio.ZIO
import zio.test._
import zio.test.Assertion._
import zio.tarantool.Generators._
import zio.tarantool.TarantoolError
import zio.test.DefaultRunnableSpec
import zio.tarantool.protocol.Implicits._

object TupleEncoderSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TupleEncoder for primitive types")(
      testM("encode/decode Unit") {
        val tupleEncoder: TupleEncoder[Unit] = TupleEncoder[Unit]

        for {
          encoded <- tupleEncoder.encodeM(())
          decoded <- tupleEncoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(new ImmutableArrayValueImpl(Array(ImmutableNilValueImpl.get())))
        ) && assert(decoded)(isUnit)
      },
      testM("encode/decode Byte") {
        checkM(byte()) { value =>
          val tupleEncoder: TupleEncoder[Byte] = TupleEncoder[Byte]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Short") {
        checkM(short()) { value =>
          val tupleEncoder: TupleEncoder[Short] = TupleEncoder[Short]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Int") {
        checkM(int()) { value =>
          val tupleEncoder: TupleEncoder[Int] = TupleEncoder[Int]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Long") {
        checkM(long()) { value =>
          val tupleEncoder: TupleEncoder[Long] = TupleEncoder[Long]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Float") {
        checkM(float()) { value =>
          val tupleEncoder: TupleEncoder[Float] = TupleEncoder[Float]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Double") {
        checkM(double()) { value =>
          val tupleEncoder: TupleEncoder[Double] = TupleEncoder[Double]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Boolean") {
        checkM(bool()) { value =>
          val tupleEncoder: TupleEncoder[Boolean] = TupleEncoder[Boolean]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode String") {
        checkM(nonEmptyString(64)) { value =>
          val tupleEncoder: TupleEncoder[String] = TupleEncoder[String]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode UUID") {
        checkM(uuid()) { value =>
          val tupleEncoder: TupleEncoder[UUID] = TupleEncoder[UUID]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode BigInt") {
        checkM(bigInt()) { value =>
          val tupleEncoder: TupleEncoder[BigInt] = TupleEncoder[BigInt]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode BigDecimal") {
        checkM(bigDecimal()) { value =>
          val tupleEncoder: TupleEncoder[BigDecimal] = TupleEncoder[BigDecimal]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Map") {
        checkM(mapOf(32, long(), int())) { value =>
          val tupleEncoder: TupleEncoder[Map[Long, Int]] = TupleEncoder[Map[Long, Int]]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode Array[Byte]") {
        checkM(nonEmptyListOf(32, byte())) { value =>
          val tupleEncoder: TupleEncoder[Array[Byte]] = TupleEncoder[Array[Byte]]
          for {
            encoded <- tupleEncoder.encodeM(value.toArray)
            decoded <- tupleEncoder.decodeM(encoded)
          } yield assert(encoded.isArrayValue)(isTrue) && assert(decoded.toList)(equalTo(value))
        }
      },
      testM("encode/decode Vector") {
        checkM(nonEmptyListOf(32, int())) { value =>
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
