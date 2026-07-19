package zio.tarantool

import java.util.UUID

import _root_.zio.test._

object Generators {
  def nonEmptyString(maxLen: Int): Gen[Any, String] =
    Gen.stringBounded(0, maxLen)(Gen.alphaNumericChar)

  def bool(): Gen[Any, Boolean] = Gen.boolean

  def listOf[A](maxSize: Int, gen: Gen[Any, A]): Gen[Any, List[A]] =
    Gen.listOfBounded(0, maxSize)(gen)

  def nonEmptyListOf[A](maxSize: Int, gen: Gen[Any, A]): Gen[Any, List[A]] =
    Gen.listOfBounded(1, maxSize)(gen)

  def mapOf[A, B](
    maxSize: Int,
    key: Gen[Any, A],
    value: Gen[Any, B]
  ): Gen[Any, Map[A, B]] =
    Gen.mapOfBounded(0, maxSize)(key, value)

  def byte(): Gen[Any, Byte] = Gen.byte

  def short(): Gen[Any, Short] = Gen.short

  def int(): Gen[Any, Int] = Gen.int

  def long(): Gen[Any, Long] = Gen.long

  def float(): Gen[Any, Float] = Gen.float

  def double(): Gen[Any, Double] = Gen.double

  def uuid(): Gen[Any, UUID] = Gen.uuid

  def bigInt(): Gen[Any, BigInt] = Gen.bigInt(BigInt(Long.MinValue), BigInt(Long.MaxValue))

  def bigDecimal(): Gen[Any, BigDecimal] =
    Gen.bigDecimal(BigDecimal(Long.MinValue), BigDecimal(Long.MaxValue))
}
