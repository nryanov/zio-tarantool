package zio.tarantool

import java.util.concurrent.TimeUnit

import scodec.{Attempt, Err}
import zio.clock.Clock
import zio.{Promise, UIO, ZIO}
import zio.tarantool.msgpack.{MessagePack, MpArray}
import zio.tarantool.protocol.TupleEncoder
import zio.tarantool.protocol.Implicits._

final case class TarantoolOperation(syncId: Long, promise: Promise[Throwable, MessagePack]) {
  def isDone: UIO[Boolean] = promise.isDone

  // todo: configurable timeout
  def messagePack: ZIO[Any with Clock, Throwable, MessagePack] =
    promise.await.timeout(zio.duration.Duration(5, TimeUnit.SECONDS)).flatMap { v =>
      ZIO.fromOption(v).orDieWith(_ => new RuntimeException("None"))
    }

  def value[A](implicit encoder: TupleEncoder[A]): ZIO[Any with Clock, Throwable, Attempt[A]] =
    messagePack.map {
      case v: MpArray => encoder.decode(v, 0)
      case v =>
        Attempt.failure(Err(s"Unexpected tuple type. Expected MpArray, but got: ${v.typeName()}"))
    }

  def valueUnsafe[A](implicit encoder: TupleEncoder[A]): ZIO[Any with Clock, Throwable, A] =
    value.map(_.require)

  def data[A](implicit
    encoder: TupleEncoder[A]
  ): ZIO[Any with Clock, Throwable, Attempt[Vector[A]]] =
    messagePack.map {
      case v: MpArray =>
        v.value.foldLeft(Attempt.successful(Vector.empty[A])) {
          case (acc, value: MpArray) =>
            for {
              a <- acc
              decodedValue <- encoder.decode(value, 0)
            } yield a :+ decodedValue
          case (_, value) =>
            Attempt.failure(
              Err(s"Unexpected tuple type. Expected MpArray, but got: ${value.typeName()}")
            )
        }
      case v =>
        Attempt.failure(Err(s"Unexpected tuple type. Expected MpArray, but got: ${v.typeName()}"))
    }

  def dataUnsafe[A](implicit
    encoder: TupleEncoder[A]
  ): ZIO[Any with Clock, Throwable, Vector[A]] =
    data.map(_.require)
}

object TarantoolOperation {
  def apply(syncId: Long, promise: Promise[Throwable, MessagePack]): TarantoolOperation =
    new TarantoolOperation(syncId, promise)
}
