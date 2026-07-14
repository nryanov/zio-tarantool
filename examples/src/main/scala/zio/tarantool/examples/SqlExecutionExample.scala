package zio.tarantool.examples

import zio._
import zio.clock.Clock
import zio.tarantool._
import zio.tarantool.codec.auto._

object SqlExecutionExample extends zio.App {
  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = (for {
    _ <- TarantoolClient.execute.sql("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))").run
    _ <- TarantoolClient.execute.sql("INSERT INTO table1 VALUES (1, 'A'), (2, 'B'), (3, 'C')").run
    resultSet <- TarantoolClient.execute
      .sql("SELECT * FROM table1")
      .run
      .flatMap(_.await.flatMap(_.resultSet[(Int, String)]))
    _ <- zio.console.putStrLn(s"Result set: $resultSet")
  } yield ExitCode.success).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (Clock.live ++ config) >>> TarantoolClient.live ++ zio.console.Console.live
  }
}
