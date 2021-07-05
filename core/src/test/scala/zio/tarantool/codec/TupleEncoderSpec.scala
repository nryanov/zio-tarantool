package zio.tarantool.codec

import scodec.bits.ByteVector
import zio.ZIO
import zio.test._
import zio.test.Assertion._
import zio.tarantool.Generators._
import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.MpFixArray
import zio.test.DefaultRunnableSpec
import zio.tarantool.protocol.Implicits._

object TupleEncoderSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TupleEncoder for primitive types")(
      testM("encode/decode unit") {
        val tupleEncoder: TupleEncoder[Unit] = TupleEncoder[Unit]

        for {
          encoded <- tupleEncoder.encodeM(())
          decoded <- tupleEncoder.decodeM(encoded, 0)
        } yield assert(encoded)(equalTo(MpFixArray(Vector.empty))) && assert(decoded)(isUnit)
      },
      testM("encode/decode byte") {
        checkM(byte()) { value =>
          val tupleEncoder: TupleEncoder[Byte] = TupleEncoder[Byte]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode short") {
        checkM(short()) { value =>
          val tupleEncoder: TupleEncoder[Short] = TupleEncoder[Short]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode int") {
        checkM(int()) { value =>
          val tupleEncoder: TupleEncoder[Int] = TupleEncoder[Int]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode long") {
        checkM(long()) { value =>
          val tupleEncoder: TupleEncoder[Long] = TupleEncoder[Long]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode float") {
        checkM(float()) { value =>
          val tupleEncoder: TupleEncoder[Float] = TupleEncoder[Float]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode double") {
        checkM(double()) { value =>
          val tupleEncoder: TupleEncoder[Double] = TupleEncoder[Double]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode boolean") {
        checkM(bool()) { value =>
          val tupleEncoder: TupleEncoder[Boolean] = TupleEncoder[Boolean]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode string") {
        checkM(nonEmptyString(64)) { value =>
          val tupleEncoder: TupleEncoder[String] = TupleEncoder[String]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode map") {
        checkM(mapOf(32, long(), int())) { value =>
          val tupleEncoder: TupleEncoder[Map[Long, Int]] = TupleEncoder[Map[Long, Int]]
          checkTupleEncoder(value, tupleEncoder)
        }
      },
      testM("encode/decode byte array") {
        checkM(nonEmptyListOf(32, byte())) { value =>
          val tupleEncoder: TupleEncoder[ByteVector] = TupleEncoder[ByteVector]
          checkTupleEncoder(ByteVector(value), tupleEncoder)
        }
      },
      testM("encode/decode vector") {
        checkM(nonEmptyListOf(32, int())) { value =>
          val tupleEncoder: TupleEncoder[Vector[Int]] = TupleEncoder[Vector[Int]]
          checkTupleEncoder(value.toVector, tupleEncoder)
        }
      }
    )

  private def checkTupleEncoder[A](
    value: A,
    tupleEncoder: TupleEncoder[A]
  ): ZIO[Any, TarantoolError.CodecError, BoolAlgebra[FailureDetails]] =
    for {
      encoded <- tupleEncoder.encodeM(value)
      decoded <- tupleEncoder.decodeM(encoded, 0)
    } yield assert(encoded.value)(hasSize(equalTo(1))) && assert(decoded)(equalTo(value))
}
