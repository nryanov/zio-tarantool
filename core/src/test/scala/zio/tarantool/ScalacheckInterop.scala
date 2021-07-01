package zio.tarantool

import zio.random.Random
import zio.{ZIO, random}
import zio.test.{Gen, Sized}
import org.scalacheck
import org.scalacheck.rng.Seed
import org.scalacheck.Gen.Parameters

// src: https://github.com/adamgfraser/zio/blob/master/test-scalacheck/shared/src/main/scala/zio/test/scalacheck/package.scala
object ScalacheckInterop {
  implicit final class ScalaCheckGenSyntax[A](private val self: scalacheck.Gen[A]) extends AnyVal {

    /**
     * Converts a legacy ScalaCheck `Gen` to a ZIO Test `Gen`.
     */
    def toGenZIO: Gen[Random with Sized, A] =
      Gen.fromEffect {
        for {
          long <- random.nextLong
          size <- Sized.size
          a <- ZIO.succeed(self.pureApply(Parameters.default.withSize(size), Seed(long)))
        } yield a
      }
  }
}
