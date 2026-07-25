package zio.tarantool.schema

final case class IndexMeta(
  spaceId: Int,
  indexId: Int,
  indexName: String,
  indexType: String,
  options: IndexOptions,
  parts: List[IndexPartMeta]
)
