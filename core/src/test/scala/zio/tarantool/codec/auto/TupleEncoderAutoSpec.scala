package zio.tarantool.codec.auto

import zio.tarantool.codec.TupleEncoder
import zio.tarantool.msgpack.{MpFixArray, MpFixMap, MpFixString, MpPositiveFixInt, MpTrue}
import zio.tarantool.codec.auto.TupleEncoderAuto._
import zio.test._
import zio.test.Assertion._
import zio.tarantool.protocol.Implicits._

object TupleEncoderAutoSpec extends DefaultRunnableSpec {
  final case class A(f1: Int, f2: Long, f3: String)
  final case class B(f1: Int, f2: Long, f3: String, f4: Vector[Int], f5: Option[Boolean])
  final case class C(f1: Int, f2: Long, f3: String, f4: Vector[Int], f5: Map[String, String])

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TupleEncoder")(
      testM("encode/decode A") {
        val value = A(1, 2L, "3")
        val encoder = TupleEncoder[A]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded, 0)
        } yield assert(encoded)(
          equalTo(
            MpFixArray(
              Vector(MpPositiveFixInt(1), MpPositiveFixInt(2), MpFixString("3"))
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM("encode/decode B") {
        val value = B(1, 2L, "3", Vector(4), Some(true))
        val encoder = TupleEncoder[B]

        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded, 0)
        } yield assert(encoded)(
          equalTo(
            MpFixArray(
              Vector(
                MpPositiveFixInt(1),
                MpPositiveFixInt(2),
                MpFixString("3"),
                MpFixArray(Vector(MpPositiveFixInt(4))),
                MpTrue
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM("encode/decode Option[A]") {
        val value: Option[A] = Some(A(1, 2L, "3"))
        val encoder = TupleEncoder[Option[A]]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded, 0)
        } yield assert(encoded)(
          equalTo(
            MpFixArray(
              Vector(MpPositiveFixInt(1), MpPositiveFixInt(2), MpFixString("3"))
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM("encode/decode Option[B]") {
        val value: Option[B] = Some(B(1, 2L, "3", Vector(4), Some(true)))
        val encoder = TupleEncoder[Option[B]]

        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded, 0)
        } yield assert(encoded)(
          equalTo(
            MpFixArray(
              Vector(
                MpPositiveFixInt(1),
                MpPositiveFixInt(2),
                MpFixString("3"),
                MpFixArray(Vector(MpPositiveFixInt(4))),
                MpTrue
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM("encode/decode C") {
        val value = C(1, 2L, "3", Vector(4), Map("5" -> "value"))
        val encoder = TupleEncoder[C]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded, 0)
        } yield assert(encoded)(
          equalTo(
            MpFixArray(
              Vector(
                MpPositiveFixInt(1),
                MpPositiveFixInt(2),
                MpFixString("3"),
                MpFixArray(Vector(MpPositiveFixInt(4))),
                MpFixMap(Map(MpFixString("5") -> MpFixString("value")))
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      }
    )
}
