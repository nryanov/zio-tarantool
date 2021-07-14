//package zio.tarantool.data
//
//import scodec.Attempt
//import zio.tarantool.codec.TupleEncoder
//import zio.tarantool.msgpack.{MessagePack, MpArray}
// fixme
//final case class TestTuple(f1: String, f2: Int, f3: Long)
//
//object TestTuple {
//  // manual codec creation example
//  implicit val tupleEncoder: TupleEncoder[TestTuple] = new TupleEncoder[TestTuple] {
//    override def encode(v: TestTuple): Attempt[MpArray] =
//      for {
//        f1Mp <- Encoder[String].encode(v.f1)
//        f2Mp <- Encoder[Int].encode(v.f2)
//        f3Mp <- Encoder[Long].encode(v.f3)
//
//        tupleMp <- Encoder[Vector[MessagePack]].encode(Vector(f1Mp, f2Mp, f3Mp))
//      } yield tupleMp.asInstanceOf[MpArray]
//
//    override def decode(v: MpArray, idx: Int): Attempt[TestTuple] = {
//      val vector = v.value
//
//      val f1Mp = Encoder[String].decode(vector(idx))
//      val f2Mp = Encoder[Int].decode(vector(idx + 1))
//      val f3Mp = Encoder[Long].decode(vector(idx + 2))
//
//      for {
//        f1 <- f1Mp
//        f2 <- f2Mp
//        f3 <- f3Mp
//      } yield TestTuple(f1, f2, f3)
//    }
//  }
//}
