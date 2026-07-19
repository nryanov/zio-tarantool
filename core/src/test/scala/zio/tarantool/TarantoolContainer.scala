package zio.tarantool

import com.dimafeng.testcontainers.GenericContainer
import _root_.zio._

object TarantoolContainer {
  def tarantool(
    imageName: String = "tarantool/tarantool:2.11-ubuntu20.04"
  ): ZLayer[Any, Nothing, GenericContainer] =
    ZLayer.scoped {
      ZIO.acquireRelease {
        ZIO.attemptBlocking {
          val container = new GenericContainer(dockerImage = imageName, exposedPorts = Seq(3301))
          container.start()
          container
        }.orDie
      }(container => ZIO.attemptBlocking(container.stop()).orDie)
    }
}
