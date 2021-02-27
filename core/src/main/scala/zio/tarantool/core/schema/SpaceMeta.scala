package zio.tarantool.core.schema

/*
  - [372, 1, '_func_index', 'memtx', 0, {}, [{'name': 'space_id', 'type': 'unsigned'},
      {'name': 'index_id', 'type': 'unsigned'}, {'name': 'func_id', 'type': 'unsigned'}]]
  - [380, 1, '_session_settings', 'service', 2, {'temporary': true}, [{'name': 'name',
        'type': 'string'}, {'name': 'value', 'type': 'any'}]]
  - [512, 0, 'test', 'memtx', 0, {}, []]
 */
private[tarantool] final case class SpaceMeta(
  spaceId: Long,
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
    spaceId: Long,
    spaceName: String,
    engine: String,
    spaceOptions: SpaceOptions,
    fieldFormat: List[FieldMeta]
  ): SpaceMeta = new SpaceMeta(spaceId, spaceName, engine, spaceOptions, fieldFormat, Map.empty)
}
