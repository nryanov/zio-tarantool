package zio.tarantool.msgpack

import scodec.{Attempt, Codec, Err}
import scodec.bits.ByteVector

trait Encoder[A] extends Serializable { self =>
  def encode(v: A): Attempt[MessagePack]

  def decode(v: MessagePack): Attempt[A]

  final def encodeUnsafe(v: A): MessagePack = encode(v).require

  final def decodeUnsafe(v: MessagePack): A = decode(v).require

  final def xmap[B](f: A => B, g: B => A): Encoder[B] = new Encoder[B] {
    override def encode(v: B): Attempt[MessagePack] = self.encode(g(v))

    override def decode(v: MessagePack): Attempt[B] = self.decode(v).map(f)
  }
}

// Adapted from: https://github.com/msgpack/msgpack-java/blob/develop/msgpack-core/src/main/java/org/msgpack/core/MessagePacker.java
object Encoder {
  def apply[A](implicit instance: Encoder[A]): instance.type = instance

  private def fail(valueType: String, value: MessagePack): Attempt[Nothing] =
    Attempt.failure(Err(s"Error while unpacking $valueType: $value"))

  implicit val messagePackEncoder: Encoder[MessagePack] = new Encoder[MessagePack] {
    override def encode(v: MessagePack): Attempt[MessagePack] = Attempt.successful(v)

    override def decode(v: MessagePack): Attempt[MessagePack] = Attempt.successful(v)
  }

  implicit val byteEncoder: Encoder[Byte] = new Encoder[Byte] {
    override def encode(v: Byte): Attempt[MessagePack] =
      if (v < -(1 << 5)) {
        Attempt.successful(MpInt8(v))
      } else {
        if (v < 0) Attempt.successful(MpNegativeFixInt(v))
        else Attempt.successful(MpPositiveFixInt(v))
      }

    override def decode(v: MessagePack): Attempt[Byte] = v match {
      case MpPositiveFixInt(value)                                            => Attempt.successful(value.toByte)
      case MpNegativeFixInt(value)                                            => Attempt.successful(value.toByte)
      case MpUint8(value)                                                     => Attempt.successful(value.toByte)
      case MpUint16(value) if value <= Byte.MaxValue                          => Attempt.successful(value.toByte)
      case MpUint32(value) if value <= Byte.MaxValue                          => Attempt.successful(value.toByte)
      case MpUint64(value) if value <= Byte.MaxValue                          => Attempt.successful(value.toByte)
      case MpInt8(value)                                                      => Attempt.successful(value.toByte)
      case MpInt16(value) if value >= Byte.MinValue && value <= Byte.MaxValue => Attempt.successful(value.toByte)
      case MpInt32(value) if value >= Byte.MinValue && value <= Byte.MaxValue => Attempt.successful(value.toByte)
      case MpInt64(value) if value >= Byte.MinValue && value <= Byte.MaxValue => Attempt.successful(value.toByte)
      case _                                                                  => fail("byte", v)
    }
  }

  implicit val shortEncoder: Encoder[Short] = new Encoder[Short] {
    override def encode(v: Short): Attempt[MessagePack] =
      if (v < -(1 << 5)) {
        if (v < -(1 << 7)) {
          Attempt.successful(MpInt16(v))
        } else {
          Attempt.successful(MpInt8(v))
        }
      } else if (v < (1 << 7)) {
        if (v < 0) Attempt.successful(MpNegativeFixInt(v))
        else Attempt.successful(MpPositiveFixInt(v))
      } else {
        if (v < (1 << 8)) {
          Attempt.successful(MpUint8(v))
        } else {
          Attempt.successful(MpUint16(v))
        }
      }

    override def decode(v: MessagePack): Attempt[Short] = v match {
      case MpPositiveFixInt(value)                                              => Attempt.successful(value.toShort)
      case MpNegativeFixInt(value)                                              => Attempt.successful(value.toShort)
      case MpUint8(value)                                                       => Attempt.successful(value.toShort)
      case MpUint16(value)                                                      => Attempt.successful(value.toShort)
      case MpUint32(value) if value <= Short.MaxValue                           => Attempt.successful(value.toShort)
      case MpUint64(value) if value <= Short.MaxValue                           => Attempt.successful(value.toShort)
      case MpInt8(value)                                                        => Attempt.successful(value.toShort)
      case MpInt16(value)                                                       => Attempt.successful(value.toShort)
      case MpInt32(value) if value >= Short.MinValue && value <= Short.MaxValue => Attempt.successful(value.toShort)
      case MpInt64(value) if value >= Short.MinValue && value <= Short.MaxValue => Attempt.successful(value.toShort)
      case _                                                                    => fail("short", v)
    }
  }

  implicit val characterEncoder: Encoder[Char] = shortEncoder.xmap(v => v.toChar, c => c.toShort)

  implicit val intEncoder: Encoder[Int] = new Encoder[Int] {
    override def encode(v: Int): Attempt[MessagePack] =
      if (v < -(1 << 5)) {
        if (v < -(1 << 15)) Attempt.successful(MpInt32(v))
        else if (v < -(1 << 7)) Attempt.successful(MpInt16(v))
        else Attempt.successful(MpInt8(v))
      } else if (v < (1 << 7)) {
        if (v >= 0) Attempt.successful(MpPositiveFixInt(v))
        else Attempt.successful(MpNegativeFixInt(v))
      } else {
        if (v < (1 << 8)) Attempt.successful(MpUint8(v))
        else if (v < (1 << 16)) Attempt.successful(MpUint16(v))
        else Attempt.successful(MpUint32(v))
      }

    override def decode(v: MessagePack): Attempt[Int] = v match {
      case MpPositiveFixInt(value)                                          => Attempt.successful(value.toInt)
      case MpNegativeFixInt(value)                                          => Attempt.successful(value.toInt)
      case MpUint8(value)                                                   => Attempt.successful(value.toInt)
      case MpUint16(value)                                                  => Attempt.successful(value.toInt)
      case MpUint32(value)                                                  => Attempt.successful(value.toInt)
      case MpUint64(value) if value <= Int.MaxValue                         => Attempt.successful(value.toInt)
      case MpInt8(value)                                                    => Attempt.successful(value.toInt)
      case MpInt16(value)                                                   => Attempt.successful(value.toInt)
      case MpInt32(value)                                                   => Attempt.successful(value.toInt)
      case MpInt64(value) if value >= Int.MinValue && value <= Int.MaxValue => Attempt.successful(value.toInt)
      case _                                                                => fail("int", v)
    }
  }

  implicit val longEncoder: Encoder[Long] = new Encoder[Long] {
    override def encode(v: Long): Attempt[MessagePack] =
      if (v < -(1L << 5)) {
        if (v < -(1L << 15)) {
          if (v < -(1L << 31)) Attempt.successful(MpInt64(v))
          else Attempt.successful(MpInt32(v.toInt))
        } else {
          if (v < -(1 << 7)) Attempt.successful(MpInt16(v.toInt))
          else Attempt.successful(MpInt8(v.toInt))
        }
      } else if (v < (1 << 7)) {
        if (v >= 0) Attempt.successful(MpPositiveFixInt(v.toInt))
        else Attempt.successful(MpNegativeFixInt(v.toInt))
      } else {
        if (v < (1L << 16)) {
          if (v < (1 << 8)) Attempt.successful(MpUint8(v.toInt))
          else Attempt.successful(MpUint16(v.toInt))
        } else {
          if (v < (1L << 32)) Attempt.successful(MpUint32(v))
          else Attempt.successful(MpUint64(v))
        }
      }

    override def decode(v: MessagePack): Attempt[Long] = v match {
      case MpPositiveFixInt(value) => Attempt.successful(value.toLong)
      case MpNegativeFixInt(value) => Attempt.successful(value.toLong)
      case MpUint8(value)          => Attempt.successful(value.toLong)
      case MpUint16(value)         => Attempt.successful(value.toLong)
      case MpUint32(value)         => Attempt.successful(value.toLong)
      case MpUint64(value)         => Attempt.successful(value.toLong)
      case MpInt8(value)           => Attempt.successful(value.toLong)
      case MpInt16(value)          => Attempt.successful(value.toLong)
      case MpInt32(value)          => Attempt.successful(value.toLong)
      case MpInt64(value)          => Attempt.successful(value.toLong)
      case _                       => fail("long", v)
    }
  }

  implicit val floatEncoder: Encoder[Float] = new Encoder[Float] {
    override def encode(v: Float): Attempt[MessagePack] = Attempt.successful(MpFloat32(v))

    override def decode(v: MessagePack): Attempt[Float] = v match {
      case MpFloat32(value) => Attempt.successful(value)
      case _                => fail("float", v)
    }
  }

  implicit val doubleEncoder: Encoder[Double] = new Encoder[Double] {
    override def encode(v: Double): Attempt[MessagePack] = Attempt.successful(MpFloat64(v))

    override def decode(v: MessagePack): Attempt[Double] = v match {
      case MpFloat32(value) => Attempt.successful(value.toDouble)
      case MpFloat64(value) => Attempt.successful(value)
      case _                => fail("double", v)
    }
  }

  implicit val stringEncoder: Encoder[String] = new Encoder[String] {
    override def encode(v: String): Attempt[MessagePack] = {
      val len = v.length
      if (len < (1 << 5)) {
        Attempt.successful(MpFixString(v))
      } else if (len < (1 << 8)) {
        Attempt.successful(MpString8(v))
      } else if (len < (1 << 16)) {
        Attempt.successful(MpString16(v))
      } else {
        Attempt.successful(MpString32(v))
      }
    }

    override def decode(v: MessagePack): Attempt[String] = v match {
      case MpFixString(value) => Attempt.successful(value)
      case MpString8(value)   => Attempt.successful(value)
      case MpString16(value)  => Attempt.successful(value)
      case MpString32(value)  => Attempt.successful(value)
      case _                  => fail("string", v)
    }
  }

  implicit val booleanEncoder: Encoder[Boolean] = new Encoder[Boolean] {
    override def encode(v: Boolean): Attempt[MessagePack] =
      if (v) Attempt.successful(MpTrue)
      else Attempt.successful(MpFalse)

    override def decode(v: MessagePack): Attempt[Boolean] = v match {
      case MpTrue  => Attempt.successful(true)
      case MpFalse => Attempt.successful(false)
      case _       => fail("boolean", v)
    }
  }

  // save it as string
  implicit val bigIntEncoder: Encoder[BigInt] = stringEncoder.xmap(s => BigInt(s), b => b.toString())

  // save it as string
  implicit val bigDecimalEncoder: Encoder[BigDecimal] = stringEncoder.xmap(s => BigDecimal(s), b => b.toString())

  implicit val binaryEncoder: Encoder[ByteVector] = new Encoder[ByteVector] {
    override def encode(v: ByteVector): Attempt[MessagePack] = {
      val len = v.length

      if (len < 256) {
        Attempt.successful(MpBinary8(v))
      } else if (len < 65535) {
        Attempt.successful(MpBinary16(v))
      } else {
        Attempt.successful(MpBinary32(v))
      }
    }

    override def decode(v: MessagePack): Attempt[ByteVector] = v match {
      case MpBinary8(value)  => Attempt.successful(value)
      case MpBinary16(value) => Attempt.successful(value)
      case MpBinary32(value) => Attempt.successful(value)
      case _                 => fail("binary", v)
    }
  }

  implicit def vectorEncoder[A: Encoder]: Encoder[Vector[A]] = new Encoder[Vector[A]] {
    override def encode(v: Vector[A]): Attempt[MessagePack] = {
      val len = v.length
      val encoder = implicitly[Encoder[A]]
      v.foldLeft(Attempt.successful(Vector.empty[MessagePack])) {
          case (acc, value) =>
            for {
              a <- acc
              encodedValue <- encoder.encode(value)
            } yield a :+ encodedValue
        }
        .map(mpVector =>
          if (len <= 15) MpFixArray(mpVector)
          else if (len <= 65535) MpArray16(mpVector)
          else MpArray32(mpVector)
        )
    }

    override def decode(v: MessagePack): Attempt[Vector[A]] = v match {
      case MpFixArray(value) => decodeVector(value)
      case MpArray16(value)  => decodeVector(value)
      case MpArray32(value)  => decodeVector(value)
      case _                 => fail("vector", v)
    }

    private def decodeVector(v: Vector[MessagePack]): Attempt[Vector[A]] = {
      val encoder = implicitly[Encoder[A]]

      v.foldLeft(Attempt.successful(Vector.empty[A])) {
        case (acc, value) =>
          for {
            a <- acc
            decodedValue <- encoder.decode(value)
          } yield a :+ decodedValue
      }
    }
  }

  implicit def mapEncoder[A: Encoder, B: Encoder]: Encoder[Map[A, B]] = new Encoder[Map[A, B]] {
    override def encode(v: Map[A, B]): Attempt[MessagePack] = {
      val len = v.size
      val keyEncoder = implicitly[Encoder[A]]
      val valueEncoder = implicitly[Encoder[B]]
      v.toSeq
        .foldLeft(Attempt.successful(Vector.empty[(MessagePack, MessagePack)])) {
          case (acc, (key, value)) =>
            for {
              a <- acc
              encodedKey <- keyEncoder.encode(key)
              encodedValue <- valueEncoder.encode(value)
            } yield a :+ (encodedKey, encodedValue)
        }
        .map(_.toMap)
        .map(mpMap =>
          if (len <= 15) MpFixMap(mpMap)
          else if (len <= 65535) MpMap16(mpMap)
          else MpMap32(mpMap)
        )
    }

    override def decode(v: MessagePack): Attempt[Map[A, B]] = v match {
      case MpFixMap(value) => decodeMap(value)
      case MpMap16(value)  => decodeMap(value)
      case MpMap32(value)  => decodeMap(value)
      case _               => fail("map", v)
    }

    private def decodeMap(map: Map[MessagePack, MessagePack]): Attempt[Map[A, B]] = {
      val keyEncoder = implicitly[Encoder[A]]
      val valueEncoder = implicitly[Encoder[B]]

      map.toSeq
        .foldLeft(Attempt.successful(Vector.empty[(A, B)])) {
          case (acc, (key, value)) =>
            for {
              a <- acc
              decodedKey <- keyEncoder.decode(key)
              decodedValue <- valueEncoder.decode(value)
            } yield a :+ (decodedKey, decodedValue)
        }
        .map(_.toMap)
    }
  }

  implicit def extensionEncoder[A](code: ByteVector)(implicit C: Codec[A]): Encoder[A] = new Encoder[A] {
    def encode(v: A): Attempt[MessagePack] =
      C.encode(v).map(_.bytes).map { encoded =>
        val len: Long = encoded.size
        if (len <= 1) MpFixExtension1(code.toInt(), encoded)
        if (len <= 2) MpFixExtension2(code.toInt(), encoded)
        if (len <= 4) MpFixExtension4(code.toInt(), encoded)
        if (len <= 8) MpFixExtension8(code.toInt(), encoded)
        if (len <= 16) MpFixExtension16(code.toInt(), encoded)
        if (len <= 256) MpExtension8(len.toInt, code.toInt(), encoded)
        else if (len <= 65536) MpExtension16(len.toInt, code.toInt(), encoded)
        else MpExtension32(len.toInt, code.toInt(), encoded)
      }

    def decode(v: MessagePack): Attempt[A] = v match {
      case MpFixExtension1(_, value)  => C.decode(value.bits).map(_.value)
      case MpFixExtension2(_, value)  => C.decode(value.bits).map(_.value)
      case MpFixExtension4(_, value)  => C.decode(value.bits).map(_.value)
      case MpFixExtension8(_, value)  => C.decode(value.bits).map(_.value)
      case MpFixExtension16(_, value) => C.decode(value.bits).map(_.value)
      case MpExtension8(_, _, value)  => C.decode(value.bits).map(_.value)
      case MpExtension16(_, _, value) => C.decode(value.bits).map(_.value)
      case MpExtension32(_, _, value) => C.decode(value.bits).map(_.value)
      case _                          => fail("extension", v)
    }
  }
}
