package zio.tarantool.internal

import _root_.zio._

/**
 * Each request should has a unique SyncId number.
 * This class is responsible to generate unique sequence of SyncId for each request per connection.
 */
private[tarantool] object SyncIdProvider {
  trait Service {
    def syncId(): UIO[Long]
  }

  def syncId(): ZIO[Service, Nothing, Long] = ZIO.serviceWithZIO(_.syncId())

  val live: ZLayer[Any, Nothing, Service] = ZLayer(make())

  def make(): UIO[Service] = Ref.make(0L).map(new Live(_))

  private[tarantool] class Live(ref: Ref[Long]) extends Service {
    override def syncId(): UIO[Long] = ref.updateAndGet(_ + 1)
  }
}
