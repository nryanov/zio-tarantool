package zio.tarantool

import com.dimafeng.testcontainers.GenericContainer
import zio.blocking.{Blocking, effectBlocking}
import zio._

object TarantoolSecuredContainer {
  type Tarantool = Has[GenericContainer]

  def tarantool(
    imageName: String = "tarantool/tarantool:2.6"
  ): ZLayer[Blocking, Nothing, Tarantool] =
    ZManaged.make {
      effectBlocking {
        val container =
          new GenericContainer(
            dockerImage = imageName,
            exposedPorts = Seq(3301),
            env = Map(
              "TARANTOOL_USER_NAME" -> "username",
              "TARANTOOL_USER_PASSWORD" -> "password"
            )
          )
        container.start()
        container
      }.orDie
    }(container => effectBlocking(container.stop()).orDie).toLayer
}
