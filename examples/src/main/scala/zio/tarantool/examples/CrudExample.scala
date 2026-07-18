package zio.tarantool.examples

import _root_.zio._
import zio.tarantool._
import zio.tarantool.codec.auto._

/**
 * Example: Tarantool 2.6
 * docker run --name=tarantool -p 3301:3301 tarantool/tarantool:2.6 -d
 * ----
 * tarantoolctl connect 3301
 * -----
 * > box.schema.create_space('users', {if_not_exists = true})
 * > box.space.users:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })
 */
object CrudExample extends ZIOAppDefault {
  final case class Address(street: String, number: Int)
  final case class User(id: Long, name: String, age: Int, address: Address)

  override def run: ZIO[Any, Any, Any] = (for {
    _ <- TarantoolClient.insert.into("users").tuple(User(1, "Name1", 10, Address("street1", 1))).run

    user <- TarantoolClient.select
      .from("users")
      .index("primary")
      .key(Tuple1(1L))
      .limit(1)
      .run
      .flatMap(_.await)
      .flatMap(_.head[User])

    // User: User(1,Name1,10,Address(street1,1))
    _ <- Console.printLine(s"User: $user")

    // currently, only primitive types supported
    updates <- user.builder.assign("name", "John").plus("age", 5).buildM()

    _ <- TarantoolClient.update.in("users").index("primary").key(Tuple1(1)).ops(updates).run

    user <- TarantoolClient.select
      .from("users")
      .index("primary")
      .key(Tuple1(1L))
      .limit(1)
      .run
      .flatMap(_.await)
      .flatMap(_.head[User])

    // User: User(1,John,15,Address(street1,1))
    _ <- Console.printLine(s"Updated user: $user")

    _ <- TarantoolClient.replace.into("users").tuple(User(1, "John Smith", 20, Address("newAddress", 10))).run

    user <- TarantoolClient.select
      .from("users")
      .index("primary")
      .key(Tuple1(1L))
      .limit(1)
      .run
      .flatMap(_.await)
      .flatMap(_.head[User])

    // User: User(1,John Smith,20,Address(newAddress,10))
    _ <- Console.printLine(s"Replaced user: $user")

    _ <- TarantoolClient.delete.from("users").index("primary").key(Tuple1(1)).run
  } yield ()).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live
  }
}
