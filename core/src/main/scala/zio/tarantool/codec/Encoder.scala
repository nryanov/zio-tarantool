package zio.tarantool.codec

import java.util.UUID

import org.msgpack.core.{MessageBufferPacker, MessageUnpacker}
import org.msgpack.value.Value

import scala.collection.mutable

trait Encoder[A] extends Serializable { self =>
  def encode(packer: MessageBufferPacker, v: A): Unit

  def decode(unpacker: MessageUnpacker): A

  final def xmap[B](f: A => B, g: B => A): Encoder[B] = new Encoder[B] {
    override def encode(packer: MessageBufferPacker, v: B): Unit = self.encode(packer, g(v))

    override def decode(unpacker: MessageUnpacker): B = f(self.decode(unpacker))
  }
}

object Encoder {
  def apply[A](implicit instance: Encoder[A]): instance.type = instance

  implicit val valueEncoder: Encoder[Value] = new Encoder[Value] {
    override def encode(packer: MessageBufferPacker, v: Value): Unit = packer.packValue(v)

    override def decode(unpacker: MessageUnpacker): Value = unpacker.unpackValue()
  }

  implicit val byteEncoder: Encoder[Byte] = new Encoder[Byte] {
    override def encode(packer: MessageBufferPacker, v: Byte): Unit = packer.packByte(v)

    override def decode(unpacker: MessageUnpacker): Byte = unpacker.unpackByte()
  }

  implicit val shortEncoder: Encoder[Short] = new Encoder[Short] {
    override def encode(packer: MessageBufferPacker, v: Short): Unit = packer.packShort(v)

    override def decode(unpacker: MessageUnpacker): Short = unpacker.unpackShort()
  }

  implicit val characterEncoder: Encoder[Char] = new Encoder[Char] {
    override def encode(packer: MessageBufferPacker, v: Char): Unit = packer.packInt(v.toInt)

    override def decode(unpacker: MessageUnpacker): Char = unpacker.unpackInt().toChar
  }

  implicit val intEncoder: Encoder[Int] = new Encoder[Int] {
    override def encode(packer: MessageBufferPacker, v: Int): Unit = packer.packInt(v)

    override def decode(unpacker: MessageUnpacker): Int = unpacker.unpackInt()
  }

  implicit val longEncoder: Encoder[Long] = new Encoder[Long] {
    override def encode(packer: MessageBufferPacker, v: Long): Unit = packer.packLong(v)

    override def decode(unpacker: MessageUnpacker): Long = unpacker.unpackLong()
  }

  implicit val floatEncoder: Encoder[Float] = new Encoder[Float] {
    override def encode(packer: MessageBufferPacker, v: Float): Unit = packer.packFloat(v)

    override def decode(unpacker: MessageUnpacker): Float = unpacker.unpackFloat()
  }

  implicit val doubleEncoder: Encoder[Double] = new Encoder[Double] {
    override def encode(packer: MessageBufferPacker, v: Double): Unit = packer.packDouble(v)

    override def decode(unpacker: MessageUnpacker): Double = unpacker.unpackDouble()
  }

  implicit val stringEncoder: Encoder[String] = new Encoder[String] {
    override def encode(packer: MessageBufferPacker, v: String): Unit = packer.packString(v)

    override def decode(unpacker: MessageUnpacker): String = unpacker.unpackString()
  }

  implicit val uuidEncoder: Encoder[UUID] =
    stringEncoder.xmap(str => UUID.fromString(str), _.toString)

  implicit val booleanEncoder: Encoder[Boolean] = new Encoder[Boolean] {
    override def encode(packer: MessageBufferPacker, v: Boolean): Unit = packer.packBoolean(v)

    override def decode(unpacker: MessageUnpacker): Boolean = unpacker.unpackBoolean()
  }

  implicit val bigIntEncoder: Encoder[BigInt] = new Encoder[BigInt] {
    override def encode(packer: MessageBufferPacker, v: BigInt): Unit =
      packer.packBigInteger(v.bigInteger)

    override def decode(unpacker: MessageUnpacker): BigInt = unpacker.unpackBigInteger()
  }

  // save it as string
  implicit val bigDecimalEncoder: Encoder[BigDecimal] = new Encoder[BigDecimal] {
    override def encode(packer: MessageBufferPacker, v: BigDecimal): Unit =
      packer.packString(v.toString())

    override def decode(unpacker: MessageUnpacker): BigDecimal = BigDecimal(unpacker.unpackString())
  }

  implicit val binaryEncoder: Encoder[Array[Byte]] = new Encoder[Array[Byte]] {
    override def encode(packer: MessageBufferPacker, v: Array[Byte]): Unit = {
      packer.packArrayHeader(v.length)
      v.foreach(b => packer.packByte(b))
    }

    override def decode(unpacker: MessageUnpacker): Array[Byte] = {
      val size = unpacker.unpackArrayHeader()
      val bytes = new Array[Byte](size)

      (0 until size).foreach(i => bytes(i) = unpacker.unpackByte())
      bytes
    }
  }

  implicit def vectorEncoder[A: Encoder]: Encoder[Vector[A]] = new Encoder[Vector[A]] {
    override def encode(packer: MessageBufferPacker, v: Vector[A]): Unit = {
      packer.packArrayHeader(v.length)
      v.foreach(a => Encoder[A].encode(packer, a))
    }

    override def decode(unpacker: MessageUnpacker): Vector[A] = {
      val size = unpacker.unpackArrayHeader()
      val buffer = mutable.ListBuffer[A]()

      (0 until size).foreach { _ =>
        val value = Encoder[A].decode(unpacker)
        buffer.append(value)
      }

      buffer.toVector
    }
  }

  implicit def mapEncoder[A: Encoder, B: Encoder]: Encoder[Map[A, B]] = new Encoder[Map[A, B]] {
    override def encode(packer: MessageBufferPacker, v: Map[A, B]): Unit = {
      packer.packMapHeader(v.size)
      v.foreach { case (a, b) =>
        Encoder[A].encode(packer, a)
        Encoder[B].encode(packer, b)
      }
    }

    override def decode(unpacker: MessageUnpacker): Map[A, B] = {
      val size = unpacker.unpackMapHeader()
      val map = mutable.Map[A, B]()

      (0 until size).foreach { _ =>
        val a = Encoder[A].decode(unpacker)
        val b = Encoder[B].decode(unpacker)
        map.put(a, b)
      }

      map.toMap
    }
  }
}
