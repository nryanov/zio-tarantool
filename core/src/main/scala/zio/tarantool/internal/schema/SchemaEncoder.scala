package zio.tarantool.internal.schema

import scodec.{Attempt, Err}
import zio.tarantool.msgpack.{Encoder, MessagePack, MpArray, MpFalse}
import zio.tarantool.protocol.TupleEncoder

object SchemaEncoder {
  implicit val indexPartMetaEncoder: TupleEncoder[IndexPartMeta] = new TupleEncoder[IndexPartMeta] {
    override def encode(v: IndexPartMeta): Attempt[MpArray] =
      Attempt.failure(Err("Not implemented"))

    override def decode(v: MpArray, idx: Int): Attempt[IndexPartMeta] = {
      val vector = v.value

      val fieldNumberMp = Encoder[Int].decode(vector(idx))
      val fieldTypeMp = Encoder[String].decode(vector(idx + 1))

      for {
        fieldNumber <- fieldNumberMp
        fieldType <- fieldTypeMp
      } yield IndexPartMeta(fieldNumber, fieldType)
    }
  }

  implicit val indexMetaEncoder: TupleEncoder[IndexMeta] = new TupleEncoder[IndexMeta] {
    override def encode(v: IndexMeta): Attempt[MpArray] = Attempt.failure(Err("Not implemented"))

    override def decode(v: MpArray, idx: Int): Attempt[IndexMeta] = {
      val vector = v.value

      val spaceIdMp = Encoder[Long].decode(vector(idx))
      val indexIdMp = Encoder[Long].decode(vector(idx + 1))
      val indexNameMp = Encoder[String].decode(vector(idx + 2))
      val indexTypeMp = Encoder[String].decode(vector(idx + 3))
      val indexOptionsMp = Encoder[Map[String, MessagePack]].decode(vector(idx + 4))
      val indexPartsMp: Attempt[Vector[MessagePack]] =
        Encoder[Vector[MessagePack]].decode(vector(idx + 5))

      for {
        spaceId <- spaceIdMp
        indexId <- indexIdMp
        indexName <- indexNameMp
        indexType <- indexTypeMp
        indexOptions <- indexOptionsMp.flatMap(decodeIndexOptions)
        indexParts <- indexPartsMp.flatMap(decodeIndexPartMeta)
      } yield IndexMeta(spaceId, indexId, indexName, indexType, indexOptions, indexParts)
    }
  }

  implicit val spaceMetaEncoder: TupleEncoder[SpaceMeta] = new TupleEncoder[SpaceMeta] {
    override def encode(v: SpaceMeta): Attempt[MpArray] = Attempt.failure(Err("Not implemented"))

    override def decode(v: MpArray, idx: Int): Attempt[SpaceMeta] = {
      val vector = v.value

      val spaceIdMp = Encoder[Long].decode(vector(idx))
      val spaceNameMp = Encoder[String].decode(vector(idx + 2))
      val engineMp = Encoder[String].decode(vector(idx + 3))
      val spaceOptionsMp = Encoder[Map[String, MessagePack]].decode(vector(idx + 5))
      val spaceFieldsMp = Encoder[Vector[Map[String, MessagePack]]].decode(vector(idx + 6))

      for {
        spaceId <- spaceIdMp
        spaceName <- spaceNameMp
        engine <- engineMp
        spaceOptions <- spaceOptionsMp.flatMap(decodeSpaceOptions)
        spaceFields <- spaceFieldsMp.flatMap(decodeFieldMeta)
      } yield SpaceMeta(spaceId, spaceName, engine, spaceOptions, spaceFields)
    }
  }

  private def decodeSpaceOptions(map: Map[String, MessagePack]): Attempt[SpaceOptions] =
    if (map.isEmpty) Attempt.successful(SpaceOptions(isTemporary = false))
    else
      Attempt.successful(
        map
          .get("temporary")
          .flatMap(mp => Encoder[Boolean].decode(mp).toOption)
          .map(SpaceOptions)
          .getOrElse(SpaceOptions(isTemporary = false))
      )

  private def decodeFieldMeta(
    vector: Vector[Map[String, MessagePack]]
  ): Attempt[List[FieldMeta]] =
    vector.map { map =>
      val fieldNameMp = Encoder[String].decode(map("name"))
      val fieldTypeMp = Encoder[String].decode(map("type"))
      val isNullableMp = Encoder[Boolean].decode(map.getOrElse("is_nullable", MpFalse))

      for {
        fieldName <- fieldNameMp
        fieldType <- fieldTypeMp
        isNullable <- isNullableMp
      } yield FieldMeta(fieldName, fieldType, isNullable)
    }.foldLeft(Attempt.successful(Vector.empty[FieldMeta])) { case (acc, value) =>
      for {
        a <- acc
        decodedValue <- value
      } yield a :+ decodedValue
    }.map(_.toList)

  private def decodeIndexOptions(map: Map[String, MessagePack]): Attempt[IndexOptions] =
    if (map.isEmpty) Attempt.successful(IndexOptions(isUnique = false))
    else
      Attempt.successful(
        map
          .get("unique")
          .flatMap(mp => Encoder[Boolean].decode(mp).toOption)
          .map(IndexOptions)
          .getOrElse(IndexOptions(isUnique = false))
      )

  private def decodeIndexPartMeta(
    vector: Vector[MessagePack]
  ): Attempt[List[IndexPartMeta]] =
    vector.map {
      case v: MpArray => indexPartMetaEncoder.decode(v, 0)
      case v =>
        Attempt.failure(
          Err(s"Unexpected tuple type. Expected MpArray, but got: ${v.typeName()}")
        )
    }.foldLeft(Attempt.successful(Vector.empty[IndexPartMeta])) { case (acc, value) =>
      for {
        a <- acc
        decodedValue <- value
      } yield a :+ decodedValue
    }.map(_.toList)
}
