package zio.tarantool.api

import org.msgpack.value.Value
import zio.tarantool.internal.schema.SpaceMeta
import zio.tarantool.protocol.{IteratorCode, TarantoolRequestBody}

final case class SelectQuery(indexId: Long, iterator: IteratorCode, limit: Long, offset: Long) {
  def encode(spaceMeta: SpaceMeta, key: Value): Map[Long, Value] =
    TarantoolRequestBody.selectBody(spaceMeta.spaceId, indexId, limit, offset, iterator, key)
}

object SelectQuery {
  val default: SelectQuery = SelectQuery(0, IteratorCode.Eq, 0, 0)

  def builder(): SelectQueryBuilder = new SelectQueryBuilder()

  final class SelectQueryBuilder {
    private var indexId: Long = 0
    private var iterator: IteratorCode = IteratorCode.Eq
    private var limit: Long = 0
    private var offset: Long = 0
  }

}
