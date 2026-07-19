package zio.tarantool.examples

import _root_.zio._
import zio.tarantool._
import zio.tarantool.codec.auto._

object SqlExecutionExample extends ZIOAppDefault {
  override def run: ZIO[Any, Any, Any] = (for {
    _ <- TarantoolClient.execute.sql("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))").run
    _ <- TarantoolClient.execute.sql("INSERT INTO table1 VALUES (1, 'A'), (2, 'B'), (3, 'C')").run
    resultSet <- TarantoolClient.execute
      .sql("SELECT * FROM table1")
      .run
      .flatMap(_.await.flatMap(_.resultSet[(Int, String)]))
    _ <- Console.printLine(s"Result set: $resultSet")
  } yield ()).provideLayer(tarantoolLayer()).orDie

  def tarantoolLayer() = {
    val config = ZLayer.succeed(TarantoolConfig(host = "localhost", port = 3301))

    (ZLayer.succeed[Clock](Clock.ClockLive) ++ config) >>> TarantoolClient.live
  }
}
