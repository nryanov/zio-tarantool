package zio.tarantool

import org.scalacheck.Gen

object Generators {
  def nonEmptyString(maxLen: Int): Gen[String] =
    Gen.alphaStr.suchThat(str => str.nonEmpty && str.length <= maxLen)

  def bool(): Gen[Boolean] = Gen.oneOf(true, false)

  def listOf[A](maxSize: Int, gen: Gen[A]): Gen[List[A]] = Gen.listOfN(maxSize, gen)

  def nonEmptyListOf[A](maxSize: Int, gen: Gen[A]): Gen[List[A]] =
    listOf(maxSize, gen).suchThat(_.nonEmpty)

  def mapOf[A, B](maxSize: Int, key: Gen[A], value: Gen[B]): Gen[Map[A, B]] =
    Gen.mapOfN(maxSize, key.flatMap(k => value.map(v => (k, v))))

  def byte(): Gen[Byte] = Gen.chooseNum(Byte.MinValue, Byte.MaxValue)

  def short(): Gen[Short] = Gen.chooseNum(Short.MinValue, Short.MaxValue)

  def int(): Gen[Int] = Gen.chooseNum(Int.MinValue, Int.MaxValue)

  def long(): Gen[Long] = Gen.chooseNum(Long.MinValue, Long.MaxValue)

  def float(): Gen[Float] = Gen.chooseNum(Float.MinValue, Float.MaxValue)

  def double(): Gen[Double] = Gen.chooseNum(Double.MinValue, Double.MaxValue)
}
