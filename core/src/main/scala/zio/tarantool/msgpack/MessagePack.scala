package zio.tarantool.msgpack

import scodec.bits.ByteVector

sealed trait MessagePack {
  def typeName(): String
}

sealed trait MpArray extends MessagePack {
  def value: Vector[MessagePack]
}

sealed trait MpMap extends MessagePack {
  def value: Map[MessagePack, MessagePack]
}

final case class MpPositiveFixInt(value: Int) extends MessagePack {
  override def typeName(): String = "MpPositiveFixInt"
}

final case class MpNegativeFixInt(value: Int) extends MessagePack {
  override def typeName(): String = "MpNegativeFixInt"
}

final case class MpFixMap(value: Map[MessagePack, MessagePack]) extends MpMap {
  override def typeName(): String = "MpFixMap"
}
final case class MpFixArray(value: Vector[MessagePack]) extends MpArray {
  override def typeName(): String = "MpFixArray"
}
final case class MpFixString(value: String) extends MessagePack {
  override def typeName(): String = "MpFixString"
}

case object MpNil extends MessagePack {
  override def typeName(): String = "MpNil"
}

sealed abstract class MpBoolean(val value: Boolean) extends MessagePack
case object MpTrue extends MpBoolean(true) {
  override def typeName(): String = "MpTrue"
}
case object MpFalse extends MpBoolean(false) {
  override def typeName(): String = "MpFalse"
}

final case class MpBinary8(value: ByteVector) extends MessagePack {
  override def typeName(): String = "MpBinary8"
}
final case class MpBinary16(value: ByteVector) extends MessagePack {
  override def typeName(): String = "MpBinary16"
}
final case class MpBinary32(value: ByteVector) extends MessagePack {
  override def typeName(): String = "MpBinary32"
}

final case class MpExtension8(size: Int, code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpExtension8"
}
final case class MpExtension16(size: Int, code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpExtension16"
}
final case class MpExtension32(size: Int, code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpExtension32"
}

final case class MpFloat32(value: Float) extends MessagePack {
  override def typeName(): String = "MpFloat32"
}
final case class MpFloat64(value: Double) extends MessagePack {
  override def typeName(): String = "MpFloat64"
}

final case class MpUint8(value: Int) extends MessagePack {
  override def typeName(): String = "MpUint8"
}
final case class MpUint16(value: Int) extends MessagePack {
  override def typeName(): String = "MpUint16"
}
final case class MpUint32(value: Long) extends MessagePack {
  override def typeName(): String = "MpUint32"
}
final case class MpUint64(value: Long) extends MessagePack {
  override def typeName(): String = "MpUint64"
}

final case class MpInt8(value: Int) extends MessagePack {
  override def typeName(): String = "MpInt8"
}
final case class MpInt16(value: Int) extends MessagePack {
  override def typeName(): String = "MpInt16"
}
final case class MpInt32(value: Int) extends MessagePack {
  override def typeName(): String = "MpInt32"
}
final case class MpInt64(value: Long) extends MessagePack {
  override def typeName(): String = "MpInt64"
}

final case class MpFixExtension1(code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpFixExtension1"
}
final case class MpFixExtension2(code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpFixExtension2"
}
final case class MpFixExtension4(code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpFixExtension4"
}
final case class MpFixExtension8(code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpFixExtension8"
}
final case class MpFixExtension16(code: Int, data: ByteVector) extends MessagePack {
  override def typeName(): String = "MpFixExtension16"
}

final case class MpString8(value: String) extends MessagePack {
  override def typeName(): String = "MpString8"
}
final case class MpString16(value: String) extends MessagePack {
  override def typeName(): String = "MpString16"
}
final case class MpString32(value: String) extends MessagePack {
  override def typeName(): String = "MpString32"
}

final case class MpArray16(value: Vector[MessagePack]) extends MpArray {
  override def typeName(): String = "MpArray16"
}
final case class MpArray32(value: Vector[MessagePack]) extends MpArray {
  override def typeName(): String = "MpArray32"
}

final case class MpMap16(value: Map[MessagePack, MessagePack]) extends MpMap {
  override def typeName(): String = "MpMap16"
}
final case class MpMap32(value: Map[MessagePack, MessagePack]) extends MpMap {
  override def typeName(): String = "MpMap32"
}
