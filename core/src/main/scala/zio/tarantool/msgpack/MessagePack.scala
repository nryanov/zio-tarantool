package zio.tarantool.msgpack

import scodec.bits.ByteVector

sealed trait MessagePack

final case class MpPositiveFixInt(value: Int) extends MessagePack

final case class MpFixMap(value: Map[MessagePack, MessagePack]) extends MessagePack
final case class MpFixArray(value: Vector[MessagePack]) extends MessagePack
final case class MpFixString(value: String) extends MessagePack

case object MpNil extends MessagePack

sealed abstract class MpBoolean(val value: Boolean) extends MessagePack
case object MpTrue extends MpBoolean(true)
case object MpFalse extends MpBoolean(false)

final case class MpBinary8(value: ByteVector) extends MessagePack
final case class MpBinary16(value: ByteVector) extends MessagePack
final case class MpBinary32(value: ByteVector) extends MessagePack

final case class MpExtension8(size: Int, code: Int, data: ByteVector) extends MessagePack
final case class MpExtension16(size: Int, code: Int, data: ByteVector) extends MessagePack
final case class MpExtension32(size: Int, code: Int, data: ByteVector) extends MessagePack

final case class MpFloat32(value: Float) extends MessagePack
final case class MpFloat64(value: Double) extends MessagePack

final case class MpUint8(value: Int) extends MessagePack
final case class MpUint16(value: Int) extends MessagePack
final case class MpUint32(value: Long) extends MessagePack
final case class MpUint64(value: Long) extends MessagePack

final case class MpInt8(value: Int) extends MessagePack
final case class MpInt16(value: Int) extends MessagePack
final case class MpInt32(value: Int) extends MessagePack
final case class MpInt64(value: Long) extends MessagePack

final case class MpFixExtension1(code: Int, data: ByteVector) extends MessagePack
final case class MpFixExtension2(code: Int, data: ByteVector) extends MessagePack
final case class MpFixExtension4(code: Int, data: ByteVector) extends MessagePack
final case class MpFixExtension8(code: Int, data: ByteVector) extends MessagePack
final case class MpFixExtension16(code: Int, data: ByteVector) extends MessagePack

final case class MpString8(value: String) extends MessagePack
final case class MpString16(value: String) extends MessagePack
final case class MpString32(value: String) extends MessagePack

final case class MpArray16(value: Vector[MessagePack]) extends MessagePack
final case class MpArray32(value: Vector[MessagePack]) extends MessagePack

final case class MpMap16(value: Map[MessagePack, MessagePack]) extends MessagePack
final case class MpMap32(value: Map[MessagePack, MessagePack]) extends MessagePack

final case class MpNegativeFixInt(value: Int) extends MessagePack
