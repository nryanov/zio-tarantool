package zio.tarantool.core.schema

private[tarantool] final case class SpaceMeta(
  spaceId: Int,
  spaceName: String,
  engine: String,
  spaceOptions: SpaceOptions,
  fieldFormat: List[FieldMeta],
  indexes: Map[String, IndexMeta]
) {
  def withIndexes(indexes: Map[String, IndexMeta]): SpaceMeta = this.copy(indexes = indexes)
}

private[tarantool] object SpaceMeta {
  def apply(
    spaceId: Int,
    spaceName: String,
    engine: String,
    spaceOptions: SpaceOptions,
    fieldFormat: List[FieldMeta]
  ): SpaceMeta = new SpaceMeta(spaceId, spaceName, engine, spaceOptions, fieldFormat, Map.empty)
}
