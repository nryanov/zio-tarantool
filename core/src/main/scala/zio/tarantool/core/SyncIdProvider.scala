package zio.tarantool.core

import zio._
import zio.macros.accessible

@accessible[SyncIdProvider.Service]
object SyncIdProvider {
  type SyncIdProvider = Has[Service]

  trait Service {
    def syncId(): UIO[Long]
  }

  val live: ZLayer[Any, Nothing, SyncIdProvider] = ZLayer.fromManaged(make())

  def make(): ZManaged[Any, Nothing, Service] = Ref.make(0L).map(new Live(_)).toManaged_

  private[this] final class Live(ref: Ref[Long]) extends Service {
    override def syncId(): UIO[Long] = ref.updateAndGet(_ + 1)
  }
}
