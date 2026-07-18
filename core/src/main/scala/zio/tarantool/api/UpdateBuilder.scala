package zio.tarantool.api

import org.msgpack.value.Value
import _root_.zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.{TarantoolResponse, UpdateOperations}
import zio.tarantool.TarantoolError

final case class UpdateBuilder private[api] (
  private val space: Option[SpaceRef] = None,
  private val index: Option[IndexRef] = None,
  private val key: Option[MpValue] = None,
  private val ops: Option[MpValue] = None
) {
  def in(spaceId: Int): UpdateBuilder = copy(space = Some(SpaceRef.Id(spaceId)))

  def in(spaceName: String): UpdateBuilder = copy(space = Some(SpaceRef.Name(spaceName)))

  def index(indexId: Int): UpdateBuilder = copy(index = Some(IndexRef.Id(indexId)))

  def index(indexName: String): UpdateBuilder = copy(index = Some(IndexRef.Name(indexName)))

  def key(key: Value): UpdateBuilder = copy(key = Some(MpValue.raw(key)))

  def key[A: TupleEncoder](key: A): UpdateBuilder = copy(key = Some(MpValue.typed(key)))

  def ops(ops: Value): UpdateBuilder = copy(ops = Some(MpValue.raw(ops)))

  def ops(ops: UpdateOperations): UpdateBuilder = copy(ops = Some(MpValue.typed(ops)))

  def run: ZIO[TarantoolClient.Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      space <- BuilderOps.require(space, "space")
      index <- BuilderOps.require(index, "index")
      key <- BuilderOps.require(key, "key")
      ops <- BuilderOps.require(ops, "ops")
      response <- BuilderOps.run(BuiltRequest.Update(space, index, key, ops))
    } yield response
}

object UpdateBuilder {
  def apply(): UpdateBuilder = new UpdateBuilder()
}
