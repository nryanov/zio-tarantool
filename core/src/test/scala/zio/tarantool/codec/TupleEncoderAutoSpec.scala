package zio.tarantool.codec

import zio.tarantool.TarantoolError.CodecError
import zio.tarantool.msgpack._
import zio.tarantool.protocol.Implicits._
import zio.tarantool.codec.auto._
import zio.test.Assertion._
import zio.test._

object TupleEncoderAutoSpec extends DefaultRunnableSpec {
  final case class A(f1: Int, f2: Long, f3: String)
  final case class B(f1: Int, f2: Long, f3: String, f4: Vector[Int], f5: Option[Boolean])
  final case class C(f1: Int, f2: Long, f3: String, f4: Vector[Int], f5: Map[String, String])
  final case class D(f1: Option[Int], f2: Option[Long], f3: Option[String])

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
      testM("encode/decode D") {
        val value = D(Some(1), None, Some("3"))
        val encoder = TupleEncoder[D]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded, 0)
        } yield assert(encoded)(
          equalTo(
            MpFixArray(
              Vector(MpPositiveFixInt(1), MpNil, MpFixString("3"))
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM(
        "should fail if some non-optional fields are MpNil when decode Type"
      ) {
        val value = MpFixArray(
          Vector(MpPositiveFixInt(1), MpNil, MpFixString("3"))
        )
        val encoder = TupleEncoder[A]
        for {
          result <- encoder.decodeM(value, 0).run
        } yield assert(result)(fails(isSubtype[CodecError](anything)))
      },
      testM(
        "should not fail and return None if some non-optional fields are MpNil when decode Option[Type]"
      ) {
        val value = MpFixArray(
          Vector(MpPositiveFixInt(1), MpNil, MpFixString("3"))
        )
        val encoder = TupleEncoder[Option[A]]
        for {
          decoded <- encoder.decodeM(value, 0)
        } yield assert(decoded)(isNone)
      },
      testM(
        "should not fail and return Some(_) if some optional fields are MpNil when decode Option[Type]"
      ) {
        val value = MpFixArray(
          Vector(MpPositiveFixInt(1), MpNil, MpFixString("3"))
        )
        val encoder = TupleEncoder[Option[D]]
        for {
          decoded <- encoder.decodeM(value, 0)
        } yield assert(decoded)(isSome(equalTo(D(Some(1), None, Some("3")))))
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
