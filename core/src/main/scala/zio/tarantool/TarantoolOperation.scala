package zio.tarantool

import zio.{Promise, UIO}
import zio.tarantool.msgpack.MessagePack

final case class TarantoolOperation(syncId: Long, promise: Promise[Throwable, MessagePack]) {
  def isDone: UIO[Boolean] = promise.isDone
}

object TarantoolOperation {
  def apply(syncId: Long, promise: Promise[Throwable, MessagePack]): TarantoolOperation = new TarantoolOperation(syncId, promise)
}
