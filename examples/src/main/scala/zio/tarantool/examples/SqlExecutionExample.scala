package zio.tarantool.examples

import zio._
import zio.clock.Clock
import zio.tarantool._
import zio.tarantool.codec.auto._

object SqlExecutionExample extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
    _ <- TarantoolClient.execute(
      "CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))"
    )
    _ <- TarantoolClient.execute("INSERT INTO table1 VALUES (1, 'A'), (2, 'B'), (3, 'C')")
    resultSet <- TarantoolClient
      .execute("SELECT * FROM table1")
      .flatMap(_.await.flatMap(_.resultSet[(Int, String)]))
    _ <- zio.console.putStrLn(s"Result set: $resultSet")
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
  }
}
