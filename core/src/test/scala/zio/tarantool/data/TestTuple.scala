package zio.tarantool.data

import scodec.Attempt
import zio.tarantool.builder.TupleBuilder
import zio.tarantool.msgpack.{Encoder, MpArray}
import zio.tarantool.protocol.TupleEncoder

final case class TestTuple(f1: String, f2: Int, f3: Long)

object TestTuple {
  implicit val tupleEncoder: TupleEncoder[TestTuple] = new TupleEncoder[TestTuple] {
    override def encode(v: TestTuple): Attempt[MpArray] =
      TupleBuilder().put(v.f1).put(v.f2).put(v.f3).build()

    override def decode(v: MpArray, idx: Int): Attempt[TestTuple] = {
      val vector = v.value

      val f1Mp = Encoder[String].decode(vector(idx))
      val f2Mp = Encoder[Int].decode(vector(idx + 1))
      val f3Mp = Encoder[Long].decode(vector(idx + 2))

      for {
        f1 <- f1Mp
        f2 <- f2Mp
        f3 <- f3Mp
      } yield TestTuple(f1, f2, f3)
    }
  }
}
