package zio.tarantool.builder

import scodec.Attempt
import zio.tarantool.msgpack._

import scala.collection.mutable

final class TupleBuilder extends Builder[Attempt[MpArray]] {
  private val buffer = mutable.ListBuffer[Attempt[MessagePack]]()

  def put[A](value: A)(implicit encoder: Encoder[A]): TupleBuilder = {
    buffer += encoder.encode(value)
    this
  }

  def build(): Attempt[MpArray] = {
    val attempt: Attempt[Vector[MessagePack]] =
      buffer.foldLeft(Attempt.successful(Vector.empty[MessagePack])) { case (acc, el) =>
        acc.flatMap(a => el.map(a :+ _))
      }

    attempt.map { vector =>
      val len = vector.size

      if (len <= 15) MpFixArray(vector)
      else if (len <= 65535) MpArray16(vector)
      else MpArray32(vector)
    }
  }
}

object TupleBuilder {
  def apply(): TupleBuilder = new TupleBuilder()
}
