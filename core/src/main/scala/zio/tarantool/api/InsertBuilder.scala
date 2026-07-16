package zio.tarantool.api

import org.msgpack.value.Value
import zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

final case class InsertBuilder private[api] (
  private val space: Option[SpaceRef] = None,
  private val tuple: Option[MpValue] = None
) {
  def into(spaceId: Int): InsertBuilder = copy(space = Some(SpaceRef.Id(spaceId)))

  def into(spaceName: String): InsertBuilder = copy(space = Some(SpaceRef.Name(spaceName)))

  def tuple(tuple: Value): InsertBuilder = copy(tuple = Some(MpValue.raw(tuple)))

  def tuple[A: TupleEncoder](tuple: A): InsertBuilder = copy(tuple = Some(MpValue.typed(tuple)))

  def run: ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      space <- BuilderOps.require(space, "space")
      tuple <- BuilderOps.require(tuple, "tuple")
      response <- BuilderOps.run(BuiltRequest.Insert(space, tuple))
    } yield response
}

object InsertBuilder {
  def apply(): InsertBuilder = new InsertBuilder()
}
