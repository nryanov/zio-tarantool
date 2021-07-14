package zio.tarantool.internal.schema

import org.msgpack.value.{ArrayValue, Value}
import org.msgpack.value.impl.{ImmutableBooleanValueImpl, ImmutableNilValueImpl}
import zio.tarantool.codec.{Encoder, TupleEncoder}

object SchemaEncoder {
  private val empty = Vector(ImmutableNilValueImpl.get())

  implicit val indexPartMetaEncoder: TupleEncoder[IndexPartMeta] = new TupleEncoder[IndexPartMeta] {

    override def decode(v: ArrayValue, idx: Int): IndexPartMeta = {
      val fieldNumberMp = Encoder[Int].decode(v.get(idx))
      val fieldTypeMp = Encoder[String].decode(v.get(idx + 1))

      IndexPartMeta(fieldNumberMp, fieldTypeMp)
    }

    override def encode(v: IndexPartMeta): Vector[Value] = empty
  }

  implicit val indexMetaEncoder: TupleEncoder[IndexMeta] = new TupleEncoder[IndexMeta] {
    override def encode(v: IndexMeta): Vector[Value] = empty

    override def decode(v: ArrayValue, idx: Int): IndexMeta = {
      val spaceIdMp = Encoder[Int].decode(v.get(idx))
      val indexIdMp = Encoder[Int].decode(v.get(idx + 1))
      val indexNameMp = Encoder[String].decode(v.get(idx + 2))
      val indexTypeMp = Encoder[String].decode(v.get(idx + 3))
      val indexOptionsMp = Encoder[Map[String, Value]].decode(v.get(idx + 4))
      val indexPartsMp: Vector[Value] = Encoder[Vector[Value]].decode(v.get(idx + 5))

      IndexMeta(
        spaceIdMp,
        indexIdMp,
        indexNameMp,
        indexTypeMp,
        decodeIndexOptions(indexOptionsMp),
        decodeIndexPartMeta(indexPartsMp)
      )
    }
  }

  implicit val spaceMetaEncoder: TupleEncoder[SpaceMeta] = new TupleEncoder[SpaceMeta] {
    override def encode(v: SpaceMeta): Vector[Value] = empty

    override def decode(v: ArrayValue, idx: Int): SpaceMeta = {
      val spaceIdMp = Encoder[Int].decode(v.get(idx))
      val spaceNameMp = Encoder[String].decode(v.get(idx + 2))
      val engineMp = Encoder[String].decode(v.get(idx + 3))
      val spaceOptionsMp = Encoder[Map[String, Value]].decode(v.get(idx + 5))
      val spaceFieldsMp = Encoder[Vector[Map[String, Value]]].decode(v.get(idx + 6))

      SpaceMeta(
        spaceIdMp,
        spaceNameMp,
        engineMp,
        decodeSpaceOptions(spaceOptionsMp),
        decodeFieldMeta(spaceFieldsMp)
      )
    }
  }

  private def decodeSpaceOptions(map: Map[String, Value]): SpaceOptions =
    if (map.isEmpty) SpaceOptions(isTemporary = false)
    else {
      val temporary = map.get("temporary")

      temporary match {
        case Some(value) => SpaceOptions(isTemporary = Encoder[Boolean].decode(value))
        case None        => SpaceOptions(isTemporary = false)
      }
    }

  private def decodeFieldMeta(vector: Vector[Map[String, Value]]): List[FieldMeta] =
    vector.map { map =>
      val fieldNameMp = Encoder[String].decode(map("name"))
      val fieldTypeMp = Encoder[String].decode(map("type"))
      val isNullableMp =
        Encoder[Boolean].decode(map.getOrElse("is_nullable", ImmutableBooleanValueImpl.FALSE))

      FieldMeta(
        fieldNameMp,
        fieldTypeMp,
        isNullableMp
      )
    }.foldLeft(Vector.empty[FieldMeta])((acc, value) => acc :+ value).toList

  private def decodeIndexOptions(map: Map[String, Value]): IndexOptions =
    if (map.isEmpty) IndexOptions(isUnique = false)
    else {
      map.get("unique") match {
        case Some(value) => IndexOptions(isUnique = Encoder[Boolean].decode(value))
        case None        => IndexOptions(isUnique = false)
      }
    }

  private def decodeIndexPartMeta(
    vector: Vector[Value]
  ): List[IndexPartMeta] =
    vector.map {
      case v: Value if v.isArrayValue => indexPartMetaEncoder.decode(v.asArrayValue(), 0)
      case v =>
        throw new IllegalArgumentException(
          s"Unexpected tuple type. Expected MpArray, but got: ${v.getValueType.name()}"
        )
    }.foldLeft(Vector.empty[IndexPartMeta])((acc, value) => acc :+ value).toList
}
