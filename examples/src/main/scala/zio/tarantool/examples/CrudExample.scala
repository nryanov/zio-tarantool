package zio.tarantool.examples

import zio._
import zio.clock.Clock
import zio.tarantool._
import zio.tarantool.codec.auto._
import zio.tarantool.protocol.IteratorCode

/**
 * Example: Tarantool 2.6
 * docker run --name=tarantool -p 3301:3301 tarantool/tarantool:2.6 -d
 * ----
 * tarantoolctl connect 3301
 * -----
 * > box.schema.create_space('users', {if_not_exists = true})
 * > box.space.users:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })
 */
object CrudExample extends zio.App {
  final case class Address(street: String, number: Int)
  final case class User(id: Long, name: String, age: Int, address: Address)

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
    _ <- TarantoolClient.insert("users", User(1, "Name1", 10, Address("street1", 1)))

    user <- TarantoolClient
      .select("users", "primary", 1, 0, IteratorCode.Eq, Tuple1(1L))
      .flatMap(_.await)
      .flatMap(_.head[User])

    // User: User(1,Name1,10,Address(street1,1))
    _ <- zio.console.putStrLn(s"User: $user")

    // currently, only primitive types supported
    updates <- user.builder.assign("name", "John").plus("age", 5).buildM()

    _ <- TarantoolClient.update("users", "primary", Tuple1(1), updates)

    user <- TarantoolClient
      .select("users", "primary", 1, 0, IteratorCode.Eq, Tuple1(1L))
      .flatMap(_.await)
      .flatMap(_.head[User])

    // User: User(1,John,15,Address(street1,1))
    _ <- zio.console.putStrLn(s"Updated user: $user")

    _ <- TarantoolClient.replace("users", User(1, "John Smith", 20, Address("newAddress", 10)))

    user <- TarantoolClient
      .select("users", "primary", 1, 0, IteratorCode.Eq, Tuple1(1L))
      .flatMap(_.await)
      .flatMap(_.head[User])

    // User: User(1,John Smith,20,Address(newAddress,10))
    _ <- zio.console.putStrLn(s"Replaced user: $user")

    _ <- TarantoolClient.delete("users", "primary", Tuple1(1))
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
  }
}
