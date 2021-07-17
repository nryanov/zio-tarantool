package zio.tarantool

import java.util.UUID

import zio.test._
import zio.random.Random

object Generators {
  def nonEmptyString(maxLen: Int): Gen[Random, String] =
    Gen.stringBounded(0, maxLen)(Gen.alphaNumericChar)

  def bool(): Gen[Random, Boolean] = Gen.boolean

  def listOf[A](maxSize: Int, gen: Gen[Random, A]): Gen[Random, List[A]] =
    Gen.listOfBounded(0, maxSize)(gen)

  def nonEmptyListOf[A](maxSize: Int, gen: Gen[Random, A]): Gen[Random, List[A]] =
    Gen.listOfBounded(1, maxSize)(gen)

  def mapOf[A, B](
    maxSize: Int,
    key: Gen[Random, A],
    value: Gen[Random, B]
  ): Gen[Random, Map[A, B]] =
    Gen.mapOfBounded(0, maxSize)(key, value)

  def byte(): Gen[Random, Byte] = Gen.anyByte

  def short(): Gen[Random, Short] = Gen.anyShort

  def int(): Gen[Random, Int] = Gen.anyInt

  def long(): Gen[Random, Long] = Gen.anyLong

  def float(): Gen[Random, Float] = Gen.anyFloat

  def double(): Gen[Random, Double] = Gen.anyDouble

  def uuid(): Gen[Random, UUID] = Gen.anyUUID

  def bigInt(): Gen[Random, BigInt] = Gen.bigInt(BigInt(Long.MinValue), BigInt(Long.MaxValue))

  def bigDecimal(): Gen[Random, BigDecimal] =
    Gen.bigDecimal(BigDecimal(Double.MinValue), BigDecimal(Double.MaxValue))
}
