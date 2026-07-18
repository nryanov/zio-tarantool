package zio.tarantool.examples

import _root_.zio._
import zio.tarantool._
import zio.tarantool.codec.auto._

/**
 * > box.schema.create_space('users', {if_not_exists = true})
 * > box.space.users:create_index('primary', {if_not_exists = true, unique = true, parts = {1, 'number'} })
 */
object HelloWorld extends ZIOAppDefault {
  final case class Address(street: String, number: Int)
  final case class User(id: Long, name: String, age: Int, address: Address)

  override def run: ZIO[Any, Any, Any] = (for {
    _ <- TarantoolClient.insert.into("users").tuple(User(1, "Name1", 10, Address("street1", 1))).run

    user <- TarantoolClient.select
      .from("users")
      .index("primary")
      .key(1L)
      .limit(1)
      .run
      .flatMap(_.await)
      .flatMap(_.head[User])

    _ <- Console.printLine(s"User: $user")
  } yield ()).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live
  }
}
