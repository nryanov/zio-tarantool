package zio.tarantool.api

import org.msgpack.value.Value
import zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

final case class DeleteBuilder private[api] (
  private val space: Option[SpaceRef] = None,
  private val index: Option[IndexRef] = None,
  private val key: Option[MpValue] = None
) {
  def from(spaceId: Int): DeleteBuilder = copy(space = Some(SpaceRef.Id(spaceId)))

  def from(spaceName: String): DeleteBuilder = copy(space = Some(SpaceRef.Name(spaceName)))

  def index(indexId: Int): DeleteBuilder = copy(index = Some(IndexRef.Id(indexId)))

  def index(indexName: String): DeleteBuilder = copy(index = Some(IndexRef.Name(indexName)))

  def key(key: Value): DeleteBuilder = copy(key = Some(MpValue.raw(key)))

  def key[A: TupleEncoder](key: A): DeleteBuilder = copy(key = Some(MpValue.typed(key)))

  def run: ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      space <- BuilderOps.require(space, "space")
      index <- BuilderOps.require(index, "index")
      key <- BuilderOps.require(key, "key")
      response <- BuilderOps.run(BuiltRequest.Delete(space, index, key))
    } yield response
}

object DeleteBuilder {
  def apply(): DeleteBuilder = new DeleteBuilder()
}
