package zio.tarantool.internal.schema

/*
  - [281, 0, 'primary', 'tree', {'unique': true}, [[0, 'unsigned']]]
  - [281, 1, 'owner', 'tree', {'unique': false}, [[1, 'unsigned']]]
  - [281, 2, 'name', 'tree', {'unique': true}, [[2, 'string']]]
 */
private[tarantool] final case class IndexMeta(
  spaceId: Long,
  indexId: Long,
  indexName: String,
  indexType: String,
  options: IndexOptions,
  parts: List[IndexPartMeta]
)
