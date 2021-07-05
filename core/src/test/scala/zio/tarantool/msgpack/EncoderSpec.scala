package zio.tarantool.msgpack

import scodec.bits.ByteVector
import zio.test._
import zio.test.Assertion._
import zio.tarantool.Generators._
import zio.test.{DefaultRunnableSpec, ZSpec}
import zio.tarantool.protocol.Implicits._

object EncoderSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] = suite("Encoder")(
    testM("encode/decode byte") {
      checkM(byte()) { value =>
        for {
          encoded <- Encoder[Byte].encodeM(value)
          decoded <- Encoder[Byte].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode short") {
      checkM(short()) { value =>
        for {
          encoded <- Encoder[Short].encodeM(value)
          decoded <- Encoder[Short].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode int") {
      checkM(int()) { value =>
        for {
          encoded <- Encoder[Int].encodeM(value)
          decoded <- Encoder[Int].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode long") {
      checkM(long()) { value =>
        for {
          encoded <- Encoder[Long].encodeM(value)
          decoded <- Encoder[Long].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode float") {
      checkM(float()) { value =>
        for {
          encoded <- Encoder[Float].encodeM(value)
          decoded <- Encoder[Float].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode double") {
      checkM(double()) { value =>
        for {
          encoded <- Encoder[Double].encodeM(value)
          decoded <- Encoder[Double].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode string") {
      checkM(nonEmptyString(64)) { value =>
        for {
          encoded <- Encoder[String].encodeM(value)
          decoded <- Encoder[String].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode boolean") {
      checkM(bool()) { value =>
        for {
          encoded <- Encoder[Boolean].encodeM(value)
          decoded <- Encoder[Boolean].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    },
    testM("encode/decode binary") {
      checkM(listOf(10, byte())) { value =>
        for {
          encoded <- Encoder[ByteVector].encodeM(ByteVector(value))
          decoded <- Encoder[ByteVector].decodeM(encoded)
        } yield assert(decoded)(equalTo(ByteVector(value)))
      }
    },
    testM("encode/decode vector") {
      checkM(listOf(32, int())) { value =>
        for {
          encoded <- Encoder[Vector[Int]].encodeM(value.toVector)
          decoded <- Encoder[Vector[Int]].decodeM(encoded)
        } yield assert(decoded)(equalTo(value.toVector))
      }
    },
    testM("encode/decode map") {
      checkM(mapOf(32, int(), int())) { value =>
        for {
          encoded <- Encoder[Map[Int, Int]].encodeM(value)
          decoded <- Encoder[Map[Int, Int]].decodeM(encoded)
        } yield assert(decoded)(equalTo(value))
      }
    }
  )
}
