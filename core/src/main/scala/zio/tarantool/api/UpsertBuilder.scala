package zio.tarantool.api

import org.msgpack.value.Value
import _root_.zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.{TarantoolResponse, UpdateOperations}
import zio.tarantool.TarantoolError

final case class UpsertBuilder private[api] (
  private val space: Option[SpaceRef] = None,
  private val index: Option[IndexRef] = None,
  private val ops: Option[MpValue] = None,
  private val tuple: Option[MpValue] = None
) {
  def into(spaceId: Int): UpsertBuilder = copy(space = Some(SpaceRef.Id(spaceId)))

  def into(spaceName: String): UpsertBuilder = copy(space = Some(SpaceRef.Name(spaceName)))

  def index(indexId: Int): UpsertBuilder = copy(index = Some(IndexRef.Id(indexId)))

  def index(indexName: String): UpsertBuilder = copy(index = Some(IndexRef.Name(indexName)))

  def ops(ops: Value): UpsertBuilder = copy(ops = Some(MpValue.raw(ops)))

  def ops(ops: UpdateOperations): UpsertBuilder = copy(ops = Some(MpValue.typed(ops)))

  def tuple(tuple: Value): UpsertBuilder = copy(tuple = Some(MpValue.raw(tuple)))

  def tuple[A: TupleEncoder](tuple: A): UpsertBuilder = copy(tuple = Some(MpValue.typed(tuple)))

  def run: ZIO[TarantoolClient.Service, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      space <- BuilderOps.require(space, "space")
      index <- BuilderOps.require(index, "index")
      ops <- BuilderOps.require(ops, "ops")
      tuple <- BuilderOps.require(tuple, "tuple")
      response <- BuilderOps.run(BuiltRequest.Upsert(space, index, ops, tuple))
    } yield response
}

object UpsertBuilder {
  def apply(): UpsertBuilder = new UpsertBuilder()
}
