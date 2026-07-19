package zio.tarantool.codec

import org.msgpack.value.impl._
import zio.tarantool.TarantoolError.CodecError
import zio.tarantool.protocol.Implicits._
import zio.tarantool.codec.auto._
import _root_.zio.test.Assertion._
import _root_.zio.test._

object TupleEncoderAutoSpec extends ZIOSpecDefault {
  final case class A(f1: Int, f2: Long, f3: String)
  final case class C(f1: Int, f2: Long, f3: String, f4: Vector[Int], f5: Map[String, String])
  final case class D(f1: Option[Int], f2: Option[Long], f3: Option[String])

  final case class Child(f1: Int, f2: String)
  final case class Parent(f1: Vector[Int], f2: Option[String], f3: Child)
  final case class NestedFirst(child: Child, flag: Boolean)

  sealed trait Shape
  final case class Circle(r: Int) extends Shape
  final case class Rect(w: Int, h: Int) extends Shape

  override def spec: Spec[TestEnvironment, Any] =
    suite("TupleEncoder")(
      test("encode/decode nested types") {
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
      test("encode/decode A") {
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
      test("encode/decode D") {
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
      test(
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
          result <- encoder.decodeM(value).exit
        } yield assert(result)(fails(isSubtype[CodecError](anything)))
      },
      test(
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
      test(
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
      test("encode/decode Option[A]") {
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
      test("encode/decode C") {
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
      },
      test("encode/decode nested case class not in last position") {
        val value = NestedFirst(Child(7, "x"), flag = true)
        val encoder = TupleEncoder[NestedFirst]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded)
        } yield assert(encoded)(
          equalTo(
            new ImmutableArrayValueImpl(
              Array(
                new ImmutableLongValueImpl(7),
                new ImmutableStringValueImpl("x"),
                ImmutableBooleanValueImpl.TRUE
              )
            )
          )
        ) && assert(decoded)(equalTo(value))
      },
      test("encode/decode sealed trait / ADT via try-decode") {
        val circle: Shape = Circle(3)
        val rect: Shape = Rect(2, 4)
        val encoder = TupleEncoder[Shape]
        for {
          encodedCircle <- encoder.encodeM(circle)
          decodedCircle <- encoder.decodeM(encodedCircle)
          encodedRect <- encoder.encodeM(rect)
          decodedRect <- encoder.decodeM(encodedRect)
        } yield assert(decodedCircle)(equalTo(circle)) && assert(decodedRect)(equalTo(rect))
      },
      test("encode/decode Option[A] None as empty vector") {
        val value: Option[A] = None
        val encoder = TupleEncoder[Option[A]]
        for {
          encoded <- encoder.encodeM(value)
          decoded <- encoder.decodeM(encoded)
        } yield assert(encoded)(equalTo(new ImmutableArrayValueImpl(Array.empty))) &&
          assert(decoded)(isNone)
      },
      test("TupleOpsBuilder maps field names to positions") {
        import zio.tarantool.protocol.FieldUpdate.SimpleFieldUpdate
        import zio.tarantool.protocol.OperatorCode

        val ops = TupleOpsBuilder[A].assign("f1", 10).plus("f2", 5L).build()
        assert(ops.map(_.ops.collect { case SimpleFieldUpdate(pos, code, _) => (pos, code) }))(
          isRight(
            equalTo(
              Vector(
                (0, OperatorCode.Assigment),
                (1, OperatorCode.Addition)
              )
            )
          )
        )
      }
    )
}
