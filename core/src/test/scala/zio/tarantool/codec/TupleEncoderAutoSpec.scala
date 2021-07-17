package zio.tarantool.codec

import org.msgpack.value.impl._
import zio.tarantool.TarantoolError.CodecError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.codec.auto._
import zio.test.Assertion._
import zio.test._

object TupleEncoderAutoSpec extends DefaultRunnableSpec {
  final case class A(f1: Int, f2: Long, f3: String)
  final case class C(f1: Int, f2: Long, f3: String, f4: Vector[Int], f5: Map[String, String])
  final case class D(f1: Option[Int], f2: Option[Long], f3: Option[String])

  final case class Child(f1: Int, f2: String)
  final case class Parent(f1: Vector[Int], f2: Option[String], f3: Child)

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("TupleEncoder")(
      testM("encode/decode nested types") {
        val value = Parent(Vector(1, 2, 3), None, Child(1, "child"))
        val encoder = TupleEncoder[Parent]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(
            new ImmutableArrayValueImpl(
              Array(
                new ImmutableArrayValueImpl(
                  Array(
                    new ImmutableLongValueImpl(1),
                    new ImmutableLongValueImpl(2),
                    new ImmutableLongValueImpl(3)
                  )
                ),
                ImmutableNilValueImpl.get(),
                new ImmutableLongValueImpl(1),
                new ImmutableStringValueImpl("child")
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM("encode/decode A") {
        val value = A(1, 2L, "3")
        val encoder = TupleEncoder[A]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(
            new ImmutableArrayValueImpl(
              Array(
                new ImmutableLongValueImpl(1),
                new ImmutableLongValueImpl(2),
                new ImmutableStringValueImpl("3")
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM("encode/decode D") {
        val value = D(Some(1), None, Some("3"))
        val encoder = TupleEncoder[D]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(
            new ImmutableArrayValueImpl(
              Array(
                new ImmutableLongValueImpl(1),
                ImmutableNilValueImpl.get(),
                new ImmutableStringValueImpl("3")
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      testM(
        "should fail if some non-optional fields are MpNil when decode Type"
      ) {
        val value = new ImmutableArrayValueImpl(
          Array(
            new ImmutableLongValueImpl(1),
            ImmutableNilValueImpl.get(),
            new ImmutableStringValueImpl("3")
          )
        )
        val encoder = TupleEncoder[A]
        for {
          result <- encoder.decodeM(value).run
        } yield assert(result)(fails(isSubtype[CodecError](anything)))
      },
      testM(
        "should not fail and return None if some non-optional fields are MpNil when decode Option[Type]"
      ) {
        val value = new ImmutableArrayValueImpl(
          Array(
            new ImmutableLongValueImpl(1),
            ImmutableNilValueImpl.get(),
            new ImmutableStringValueImpl("3")
          )
        )
        val encoder = TupleEncoder[Option[A]]
        for {
          decoded <- encoder.decodeM(value)
        } yield assert(decoded)(isNone)
      },
      testM(
        "should not fail and return Some(_) if some optional fields are MpNil when decode Option[Type]"
      ) {
        val value = new ImmutableArrayValueImpl(
          Array(
            new ImmutableLongValueImpl(1),
            ImmutableNilValueImpl.get(),
            new ImmutableStringValueImpl("3")
          )
        )
        val encoder = TupleEncoder[Option[D]]
        for {
          decoded <- encoder.decodeM(value)
        } yield assert(decoded)(isSome(equalTo(D(Some(1), None, Some("3")))))
      },
      testM("encode/decode Option[A]") {
        val value: Option[A] = Some(A(1, 2L, "3"))
        val encoder = TupleEncoder[Option[A]]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(
            new ImmutableArrayValueImpl(
              Array(
                new ImmutableLongValueImpl(1),
                new ImmutableLongValueImpl(2),
                new ImmutableStringValueImpl("3")
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
          decoded <- encoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(
            new ImmutableArrayValueImpl(
              Array(
                new ImmutableLongValueImpl(1),
                new ImmutableLongValueImpl(2),
                new ImmutableStringValueImpl("3"),
                new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(4))),
                new ImmutableMapValueImpl(
                  Array(
                    new ImmutableStringValueImpl("5"),
                    new ImmutableStringValueImpl("value")
                  )
                )
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      }
    )
}
