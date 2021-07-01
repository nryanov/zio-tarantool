package zio.tarantool.core

import zio._
import zio.macros.accessible

/**
 * Each request should has a unique SyncId number.
 * This class is responsible to generate unique sequence of SyncId for each request per connection.
 */
@accessible[SyncIdProvider.Service]
private[tarantool] object SyncIdProvider {
  type SyncIdProvider = Has[Service]

  trait Service {
    def syncId(): UIO[Long]
  }

  val live: ZLayer[Any, Nothing, SyncIdProvider] = ZLayer.fromManaged(make())

  def make(): ZManaged[Any, Nothing, Service] = Ref.make(0L).map(new Live(_)).toManaged_

  private[tarantool] class Live(ref: Ref[Long]) extends Service {
    override def syncId(): UIO[Long] = ref.updateAndGet(_ + 1)
  }
}
