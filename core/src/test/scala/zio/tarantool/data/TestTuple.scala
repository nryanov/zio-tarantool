package zio.tarantool.data

import org.msgpack.value.{ArrayValue, Value}
import zio.tarantool.codec.{Encoder, TupleEncoder}

final case class TestTuple(f1: String, f2: Int, f3: Long)

object TestTuple {
  // manual codec creation example
  implicit val tupleEncoder: TupleEncoder[TestTuple] = new TupleEncoder[TestTuple] {

    override def decode(v: ArrayValue, idx: Int): TestTuple = {
      val f1Mp = Encoder[String].decode(v.get(idx))
      val f2Mp = Encoder[Int].decode(v.get(idx + 1))
      val f3Mp = Encoder[Long].decode(v.get(idx + 2))
      TestTuple(f1Mp, f2Mp, f3Mp)
    }

    override def encode(v: TestTuple): Vector[Value] = {
      val f1Mp = Encoder[String].encode(v.f1)
      val f2Mp = Encoder[Int].encode(v.f2)
      val f3Mp = Encoder[Long].encode(v.f3)

      Vector(f1Mp, f2Mp, f3Mp)
    }
  }
}
