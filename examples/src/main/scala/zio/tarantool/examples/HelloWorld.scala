package zio.tarantool.examples

import zio._
import zio.clock.Clock
import zio.tarantool._
import zio.tarantool.codec.auto._
import zio.tarantool.protocol.IteratorCode

/**
 * > box.schema.create_space('users', {if_not_exists = true})
 * > box.space.users:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })
 */
object HelloWorld extends zio.App {
  final case class Address(street: String, number: Int)
  final case class User(id: Long, name: String, age: Int, address: Address)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
    _ <- TarantoolClient.insert("users", User(1, "Name1", 10, Address("street1", 1)))

    user <- TarantoolClient
      .select("users", "primary", 1, 0, IteratorCode.Eq, 1L)
      .flatMap(_.await)
      .flatMap(_.head[User])

    _ <- zio.console.putStrLn(s"User: $user")
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
  }
}
