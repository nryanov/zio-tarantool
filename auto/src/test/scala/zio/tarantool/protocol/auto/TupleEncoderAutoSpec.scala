package zio.tarantool.protocol.auto

import org.scalatest.OptionValues
import zio.tarantool.BaseSpec
import zio.tarantool.msgpack.{MpFixArray, MpFixString, MpPositiveFixInt, MpTrue}
import zio.tarantool.protocol.TupleEncoder
import zio.tarantool.protocol.auto.TupleEncoderAuto._

class TupleEncoderAutoSpec extends BaseSpec with OptionValues {
  final case class A(f1: Int, f2: Long, f3: String)
  final case class B(f1: Int, f2: Long, f3: String, f4: Vector[Int], f5: Option[Boolean])

  "TupleEncoder" should {
    "encode/decode A" in {
      val value = A(1, 2L, "3")
      val encoder = TupleEncoder[A]
      val encoded = encoder.encode(value).toOption.value
      val decoded = encoder.decode(encoded, 0).toOption.value

      encoded shouldBe MpFixArray(
        Vector(MpPositiveFixInt(1), MpPositiveFixInt(2), MpFixString("3"))
      )
      decoded shouldBe value
    }

    "encode/decode B" in {
      val value = B(1, 2L, "3", Vector(4), Some(true))
      val encoder = TupleEncoder[B]
      val encoded = encoder.encode(value).toOption.value
      val decoded = encoder.decode(encoded, 0).toOption.value

      encoded shouldBe MpFixArray(
        Vector(
          MpPositiveFixInt(1),
          MpPositiveFixInt(2),
          MpFixString("3"),
          MpFixArray(Vector(MpPositiveFixInt(4))),
          MpTrue
        )
      )
      decoded shouldBe value
    }

    "encode/decode Option[A]" in {
      val value: Option[A] = Some(A(1, 2L, "3"))
      val encoder = TupleEncoder[Option[A]]
      val encoded = encoder.encode(value).toOption.value
      val decoded = encoder.decode(encoded, 0).toOption.value

      encoded shouldBe MpFixArray(
        Vector(MpPositiveFixInt(1), MpPositiveFixInt(2), MpFixString("3"))
      )
      decoded shouldBe value
    }

    "encode/decode Option[B]" in {
      val value: Option[B] = Some(B(1, 2L, "3", Vector(4), Some(true)))
      val encoder = TupleEncoder[Option[B]]
      val encoded = encoder.encode(value).toOption.value
      val decoded = encoder.decode(encoded, 0).toOption.value

      encoded shouldBe MpFixArray(
        Vector(
          MpPositiveFixInt(1),
          MpPositiveFixInt(2),
          MpFixString("3"),
          MpFixArray(Vector(MpPositiveFixInt(4))),
          MpTrue
        )
      )
      decoded shouldBe value
    }
  }
}
