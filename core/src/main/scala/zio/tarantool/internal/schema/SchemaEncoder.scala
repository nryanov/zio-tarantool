package zio.tarantool.internal.schema

import org.msgpack.value.{ArrayValue, MapValue, Value}
import org.msgpack.value.impl.{ImmutableBooleanValueImpl, ImmutableNilValueImpl}
import zio.tarantool.codec.{Encoder, TupleEncoder}
import zio.tarantool.internal.schema.IndexPartMeta.{FullIndexPartMeta, SimpleIndexPartMeta}

private[tarantool] object SchemaEncoder {
  private val empty = Vector(ImmutableNilValueImpl.get())

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
      case v: Value if v.isArrayValue => decodeSimpleIndexMeta(v.asArrayValue())
      case v: Value if v.isMapValue   => decodeFullIndexMeta(v.asMapValue())

      case v =>
        throw new IllegalArgumentException(
          s"Unexpected tuple type. Expected MpArray, but got: ${v.getValueType.name()}"
        )
    }.foldLeft(Vector.empty[IndexPartMeta])((acc, value) => acc :+ value).toList

  private def decodeSimpleIndexMeta(v: ArrayValue): SimpleIndexPartMeta = {
    val fieldNumberMp = Encoder[Int].decode(v.get(0))
    val fieldTypeMp = Encoder[String].decode(v.get(1))

    SimpleIndexPartMeta(fieldNumberMp, fieldTypeMp)
  }

  private def decodeFullIndexMeta(v: MapValue): FullIndexPartMeta = {
    val map = Encoder[Map[String, Value]].decode(v)

    FullIndexPartMeta(
      fieldType = Encoder[String].decode(map("type")),
      fieldNumber = Encoder[Int].decode(map("field")),
      isNullable = Encoder[Boolean].decode(map("is_nullable")),
      nullableAction = Encoder[String].decode(map("nullable_action")),
      sortOrder = Encoder[String].decode(map("sort_order"))
    )
  }

}
