package zio.tarantool.internal.schema

private[tarantool] final case class IndexMeta(
  spaceId: Int,
  indexId: Int,
  indexName: String,
  indexType: String,
  options: IndexOptions,
  parts: List[IndexPartMeta]
)
