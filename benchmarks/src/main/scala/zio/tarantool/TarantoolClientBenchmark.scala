package zio.tarantool

import java.util.concurrent.TimeUnit

import org.msgpack.value._
import org.msgpack.value.impl._
import org.openjdk.jmh.annotations._
import _root_.zio.ZIO
import zio.tarantool.TarantoolClient.Service
import zio.tarantool.TarantoolClientBenchmark.A

@Fork(1)
@State(Scope.Thread)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.SECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class TarantoolClientBenchmark extends BenchmarkBase {
  @Param(Array("1000"))
  var count: Int = _

  private var msgpackValues: List[Value] = _

  private var notEncodedValues: List[A] = _

  private var spaceId: Int = _

  @Setup(Level.Trial)
  def setup(): Unit = {
    val init: ZIO[TarantoolClient, TarantoolError, Int] = for {
      space <- TarantoolClient.eval.expression("box.schema.create_space('A', {if_not_exists = true})").run
      ids <- TarantoolClient.eval.expression("box.schema.sequence.create('ids', {if_not_exists = true})").run
      index <- TarantoolClient.eval
        .expression("box.space.A:create_index('primary', {if_not_exists = true, sequence='ids'})")
        .run

      spaceIdReq <- TarantoolClient.eval.expression("return box.space.A.id").run
      spaceId <- spaceIdReq.await.flatMap(_.head[Int])
      _ <- space.await
      _ <- ids.await
      _ <- index.await
    } yield spaceId

    spaceId = zioUnsafeRun(init)

    msgpackValues = (0 to count).map { i =>
      new ImmutableArrayValueImpl(
        Array(
          ImmutableNilValueImpl.get(),
          new ImmutableLongValueImpl(i.toLong),
          new ImmutableStringValueImpl(s"value: $i")
        )
      )
    }.toList

    notEncodedValues = (0 to count).map(i => A(None, i.toLong, s"value: $i")).toList
  }

  @TearDown
  def tearDown(): Unit =
    zioUnsafeRun(
      TarantoolClient.eval.expression("box.space.A:truncate()").run.flatMap(_.await.unit)
    )

  @Benchmark
  def insertMsgpackValues(): Unit =
    zioUnsafeRun(
      ZIO
        .foreach(msgpackValues)(v => TarantoolClient.insert.into(spaceId).tuple(v).run)
        .flatMap(promises => ZIO.foreach_(promises)(_.await))
    )

  @Benchmark
  def insertNotEncodedValues(): Unit = {
    import zio.tarantool.codec.auto._

    zioUnsafeRun(
      ZIO
        .foreach(notEncodedValues)(v => TarantoolClient.insert.into(spaceId).tuple(v).run)
        .flatMap(promises => ZIO.foreach_(promises)(_.await))
    )
  }
}

object TarantoolClientBenchmark {
  final case class A(id: Option[Long], f1: Long, f3: String)
}
