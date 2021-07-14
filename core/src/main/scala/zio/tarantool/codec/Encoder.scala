package zio.tarantool.codec

import java.util.UUID

import org.msgpack.value.Value
import org.msgpack.value.impl._

import scala.jdk.CollectionConverters._

trait Encoder[A] extends Serializable { self =>
  def encode(v: A): Value

  def decode(value: Value): A

  final def xmap[B](f: A => B, g: B => A): Encoder[B] = new Encoder[B] {
    override def encode(v: B): Value = self.encode(g(v))

    override def decode(value: Value): B = f(self.decode(value))
  }
}

object Encoder {
  def apply[A](implicit instance: Encoder[A]): instance.type = instance

  implicit val valueEncoder: Encoder[Value] = new Encoder[Value] {
    override def encode(v: Value): Value = v

    override def decode(value: Value): Value = value
  }

  implicit val byteEncoder: Encoder[Byte] = new Encoder[Byte] {
    override def encode(v: Byte): Value = new ImmutableLongValueImpl(v.toLong)

    override def decode(value: Value): Byte = value.asNumberValue().toByte
  }

  implicit val shortEncoder: Encoder[Short] = new Encoder[Short] {
    override def encode(v: Short): Value = new ImmutableLongValueImpl(v.toLong)

    override def decode(value: Value): Short = value.asNumberValue().toShort
  }

  implicit val characterEncoder: Encoder[Char] = new Encoder[Char] {
    override def encode(v: Char): Value = new ImmutableLongValueImpl(v.toLong)

    override def decode(value: Value): Char = value.asNumberValue().toInt.toChar
  }

  implicit val intEncoder: Encoder[Int] = new Encoder[Int] {
    override def encode(v: Int): Value = new ImmutableLongValueImpl(v.toLong)

    override def decode(value: Value): Int = value.asNumberValue().toInt
  }

  implicit val longEncoder: Encoder[Long] = new Encoder[Long] {
    override def encode(v: Long): Value = new ImmutableLongValueImpl(v)

    override def decode(value: Value): Long = value.asNumberValue().toLong
  }

  implicit val floatEncoder: Encoder[Float] = new Encoder[Float] {
    override def encode(v: Float): Value = new ImmutableDoubleValueImpl(v.toDouble)

    override def decode(value: Value): Float = value.asNumberValue().toFloat
  }

  implicit val doubleEncoder: Encoder[Double] = new Encoder[Double] {
    override def encode(v: Double): Value = new ImmutableDoubleValueImpl(v)

    override def decode(value: Value): Double = value.asNumberValue().toDouble
  }

  implicit val stringEncoder: Encoder[String] = new Encoder[String] {
    override def encode(v: String): Value = new ImmutableStringValueImpl(v)

    override def decode(value: Value): String = value.asStringValue().asString()
  }

  implicit val uuidEncoder: Encoder[UUID] =
    stringEncoder.xmap(str => UUID.fromString(str), _.toString)

  implicit val booleanEncoder: Encoder[Boolean] = new Encoder[Boolean] {
    override def encode(v: Boolean): Value =
      if (v) ImmutableBooleanValueImpl.TRUE else ImmutableBooleanValueImpl.FALSE

    override def decode(value: Value): Boolean = value.asBooleanValue().getBoolean
  }

  implicit val bigIntEncoder: Encoder[BigInt] = new Encoder[BigInt] {
    override def encode(v: BigInt): Value = new ImmutableBigIntegerValueImpl(v.bigInteger)

    override def decode(value: Value): BigInt = value.asNumberValue().toBigInteger
  }

  // save it as string
  implicit val bigDecimalEncoder: Encoder[BigDecimal] = new Encoder[BigDecimal] {
    override def encode(v: BigDecimal): Value = new ImmutableStringValueImpl(v.toString())

    override def decode(value: Value): BigDecimal = BigDecimal(value.asStringValue().asString())
  }

  implicit val binaryEncoder: Encoder[Array[Byte]] = new Encoder[Array[Byte]] {
    override def encode(v: Array[Byte]): Value =
      new ImmutableArrayValueImpl(v.map(b => new ImmutableLongValueImpl(b.toLong)))

    override def decode(value: Value): Array[Byte] =
      value.asArrayValue().list().asScala.map(_.asNumberValue().toByte).toArray
  }

  implicit def vectorEncoder[A: Encoder]: Encoder[Vector[A]] = new Encoder[Vector[A]] {
    override def encode(v: Vector[A]): Value =
      new ImmutableArrayValueImpl(v.map(Encoder[A].encode).toArray)

    override def decode(value: Value): Vector[A] =
      value.asArrayValue().list().asScala.map(Encoder[A].decode).toVector
  }

  implicit def mapEncoder[A: Encoder, B: Encoder]: Encoder[Map[A, B]] = new Encoder[Map[A, B]] {
    override def encode(v: Map[A, B]): Value =
      new ImmutableMapValueImpl(
        v.flatMap { case (a, b) =>
          Seq(Encoder[A].encode(a), Encoder[B].encode(b))
        }.toArray
      )

    override def decode(value: Value): Map[A, B] =
      value
        .asMapValue()
        .map()
        .asScala
        .map { case (key, value) =>
          Encoder[A].decode(key) -> Encoder[B].decode(value)
        }
        .toMap
  }
}
