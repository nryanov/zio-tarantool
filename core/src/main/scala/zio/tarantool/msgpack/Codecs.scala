package zio.tarantool.msgpack

import scodec.Codec
import scodec.bits._
import scodec.codecs._
import shapeless.HNil

object Codecs {
  implicit val mpPositiveFixIntCodec: Codec[MpPositiveFixInt] =
    (constant(bin"0") :: uint(7)).dropUnits.as[MpPositiveFixInt]
  implicit val mpNegativeFixIntCodec: Codec[MpNegativeFixInt] =
    (constant(bin"111") :: int(5)).dropUnits.as[MpNegativeFixInt]
  implicit val mpFixMapCodec: Codec[MpFixMap] =
    (constant(bin"1000") :: multiMapCodec(uint(4))).dropUnits.as[MpFixMap]
  implicit val mpFixArrayCodec: Codec[MpFixArray] =
    (constant(bin"1001") :: arrayCodec(uint(4))).dropUnits.as[MpFixArray]
  implicit val mpFixStringCodec: Codec[MpFixString] =
    (constant(bin"101") :: variableSizeBytes(uint(5), utf8)).dropUnits.as[MpFixString]
  implicit val mpNilCodec: Codec[MpNil.type] = constant(bin"11000000").dropLeft(provide(MpNil))
  implicit val mpFalseCodec: Codec[MpFalse.type] =
    constant(bin"11000010").dropLeft(provide(MpFalse))
  implicit val mpTrueCodec: Codec[MpTrue.type] = constant(bin"11000011").dropLeft(provide(MpTrue))
  implicit val mpBooleanCodec: Codec[MpBoolean] = lazily(Codec.coproduct[MpBoolean].choice)
  implicit val mpBinary8Codec: Codec[MpBinary8] =
    (constant(bin"11000100") :: variableSizeBytes(uint8, bytes)).dropUnits.as[MpBinary8]
  implicit val mpBinary16Codec: Codec[MpBinary16] =
    (constant(bin"11000101") :: variableSizeBytes(uint16, bytes)).dropUnits.as[MpBinary16]
  implicit val mpBinary32Codec: Codec[MpBinary32] =
    (constant(bin"11000110") :: variableSizeBytesLong(uint32, bytes)).dropUnits.as[MpBinary32]
  implicit val mpExtension8Codec: Codec[MpExtension8] =
    (constant(hex"C7") :: extensionVariableSize(uint8)).dropUnits.as[MpExtension8]
  implicit val mpExtension16Codec: Codec[MpExtension16] =
    (constant(hex"C8") :: extensionVariableSize(uint8)).dropUnits.as[MpExtension16]
  implicit val mpExtension32Codec: Codec[MpExtension32] =
    (constant(hex"C9") :: extensionVariableSize(uint8)).dropUnits.as[MpExtension32]
  implicit val mpFloat32Codec: Codec[MpFloat32] =
    (constant(hex"CA") :: float).dropUnits.as[MpFloat32]
  implicit val mpFloat64Codec: Codec[MpFloat64] =
    (constant(hex"CB") :: double).dropUnits.as[MpFloat64]
  implicit val mpUint8Codec: Codec[MpUint8] = (constant(hex"CC") :: uint8).dropUnits.as[MpUint8]
  implicit val mpUint16Codec: Codec[MpUint16] = (constant(hex"CD") :: uint16).dropUnits.as[MpUint16]
  implicit val mpUint32Codec: Codec[MpUint32] = (constant(hex"CE") :: uint32).dropUnits.as[MpUint32]
  implicit val mpUint64Codec: Codec[MpUint64] =
    (constant(hex"CF") :: long(64)).dropUnits.xmap(a => MpUint64(a.head), v => v.value :: HNil)
  implicit val mpInt8Codec: Codec[MpInt8] = (constant(hex"D0") :: int8).dropUnits.as[MpInt8]
  implicit val mpInt16Codec: Codec[MpInt16] = (constant(hex"D1") :: int16).dropUnits.as[MpInt16]
  implicit val mpInt32Codec: Codec[MpInt32] = (constant(hex"D2") :: int32).dropUnits.as[MpInt32]
  implicit val mpInt64Codec: Codec[MpInt64] = (constant(hex"D3") :: int64).dropUnits.as[MpInt64]
  implicit val mpFixExtension1Codec: Codec[MpFixExtension1] =
    (constant(hex"D4") :: int8 :: bytes(1)).dropUnits.as[MpFixExtension1]
  implicit val mpFixExtension2Codec: Codec[MpFixExtension2] =
    (constant(hex"D5") :: int8 :: bytes(2)).dropUnits.as[MpFixExtension2]
  implicit val mpFixExtension4Codec: Codec[MpFixExtension4] =
    (constant(hex"D6") :: int8 :: bytes(4)).dropUnits.as[MpFixExtension4]
  implicit val mpFixExtension8Codec: Codec[MpFixExtension8] =
    (constant(hex"D7") :: int8 :: bytes(8)).dropUnits.as[MpFixExtension8]
  implicit val mpFixExtension16Codec: Codec[MpFixExtension16] =
    (constant(hex"D8") :: int8 :: bytes(16)).dropUnits.as[MpFixExtension16]
  implicit val mpString8Codec: Codec[MpString8] =
    (constant(hex"D9") :: variableSizeBytes(uint8, utf8)).dropUnits.as[MpString8]
  implicit val mpString16Codec: Codec[MpString16] =
    (constant(hex"DA") :: variableSizeBytes(uint16, utf8)).dropUnits.as[MpString16]
  implicit val mpString32Codec: Codec[MpString32] =
    (constant(hex"DB") :: variableSizeBytesLong(uint32, utf8)).dropUnits.as[MpString32]
  implicit val mpArray16Codec: Codec[MpArray16] =
    (constant(hex"DC") :: arrayCodec(uint16)).dropUnits.as[MpArray16]
  implicit val mpArray32Codec: Codec[MpArray32] =
    (constant(hex"DD") :: arrayCodec(uint32.xmap(_.toInt, _.toLong))).dropUnits.as[MpArray32]
  implicit val mpMap16Codec: Codec[MpMap16] =
    (constant(hex"DE") :: multiMapCodec(uint16)).dropUnits.as[MpMap16]
  implicit val mpMap32Codec: Codec[MpMap32] =
    (constant(hex"DF") :: multiMapCodec(uint32.xmap(_.toInt, _.toLong))).dropUnits.as[MpMap32]
  implicit val mpArrayCodec: Codec[MpArray] = lazily(Codec.coproduct[MpArray].choice)
  implicit val mpMapCodec: Codec[MpMap] = lazily(Codec.coproduct[MpMap].choice)

  private def multiMapCodec(size: Codec[Int]): Codec[Map[MessagePack, MessagePack]] =
    vectorOfN(size, MessagePackCodec ~ MessagePackCodec).xmap(vec => vec.toMap, map => map.toVector)

  private def arrayCodec(size: Codec[Int]): Codec[Vector[MessagePack]] =
    vectorOfN(size, MessagePackCodec)

  private def extensionVariableSize(size: Codec[Int]) =
    size.flatPrepend(n => int8 :: bytes(n))
}
