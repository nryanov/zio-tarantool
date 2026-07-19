package zio.tarantool.api

import org.msgpack.value.Value
import _root_.zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.{IteratorCode, TarantoolResponse}
import zio.tarantool.TarantoolError

final case class SelectBuilder private[api] (
  private val space: Option[SpaceRef] = None,
  private val index: Option[IndexRef] = None,
  private val limit: Option[Int] = None,
  private val offset: Int = 0,
  private val iterator: IteratorCode = IteratorCode.Eq,
  private val key: Option[MpValue] = None
) {
  def from(spaceId: Int): SelectBuilder = copy(space = Some(SpaceRef.Id(spaceId)))

  def from(spaceName: String): SelectBuilder = copy(space = Some(SpaceRef.Name(spaceName)))

  def index(indexId: Int): SelectBuilder = copy(index = Some(IndexRef.Id(indexId)))

  def index(indexName: String): SelectBuilder = copy(index = Some(IndexRef.Name(indexName)))

  def limit(limit: Int): SelectBuilder = copy(limit = Some(limit))

  def offset(offset: Int): SelectBuilder = copy(offset = offset)

  def iterator(iterator: IteratorCode): SelectBuilder = copy(iterator = iterator)

  def eq: SelectBuilder = copy(iterator = IteratorCode.Eq)

  def req: SelectBuilder = copy(iterator = IteratorCode.Req)

  def all: SelectBuilder = copy(iterator = IteratorCode.All)

  def lt: SelectBuilder = copy(iterator = IteratorCode.Lt)

  def le: SelectBuilder = copy(iterator = IteratorCode.Le)

  def ge: SelectBuilder = copy(iterator = IteratorCode.Ge)

  def gt: SelectBuilder = copy(iterator = IteratorCode.Gt)

  def key(key: Value): SelectBuilder = copy(key = Some(MpValue.raw(key)))

  def key[A: TupleEncoder](key: A): SelectBuilder = copy(key = Some(MpValue.typed(key)))

  def run: ZIO[TarantoolClient.Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      space <- BuilderOps.require(space, "space")
      index <- BuilderOps.require(index, "index")
      limit <- BuilderOps.require(limit, "limit")
      key <- BuilderOps.require(key, "key")
      response <- BuilderOps.run(BuiltRequest.Select(space, index, limit, offset, iterator, key))
    } yield response
}

object SelectBuilder {
  def apply(): SelectBuilder = new SelectBuilder()
}
