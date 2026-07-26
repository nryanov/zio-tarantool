package zio.tarantool.schema

import _root_.zio.ZIO
import zio.tarantool.TarantoolClient
import zio.tarantool.TarantoolError

object SchemaApi {
  def createSpace(name: String): CreateSpaceBuilder = CreateSpaceBuilder(name)

  def createIndex(space: String, name: String): CreateIndexBuilder =
    CreateIndexBuilder(space, name)

  def dropSpace(name: String): DropSpaceBuilder = DropSpaceBuilder(name)

  def dropIndex(space: String, name: String): DropIndexBuilder =
    DropIndexBuilder(space, name)

  def truncate(space: String): TruncateBuilder = TruncateBuilder(space)

  def refresh(): ZIO[TarantoolClient.Service, TarantoolError, Unit] =
    TarantoolClient.refreshMeta()

  def spaceMeta(name: String): ZIO[TarantoolClient.Service, TarantoolError, SpaceMeta] =
    ZIO.serviceWithZIO(_.spaceMeta(name))

  def indexMeta(
    space: String,
    index: String
  ): ZIO[TarantoolClient.Service, TarantoolError, IndexMeta] =
    ZIO.serviceWithZIO(_.indexMeta(space, index))
}
