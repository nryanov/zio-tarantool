package zio.tarantool.protocol

import zio._
import scodec.{Attempt, Err}
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.msgpack.{MessagePack, MpArray}

/**
 * @param messagePack - data returned in response
 */
// todo: separate class for eval and sql?
final case class TarantoolResponse(messagePack: MessagePack) {

  // todo: return IO[???, A]
  /** use this method to get actual value after `eval` */
  def value[A](implicit encoder: TupleEncoder[A]): Task[Attempt[A]] =
    ZIO.effect(messagePack match {
      case v: MpArray => encoder.decode(v, 0)
      case v =>
        Attempt.failure(Err(s"Unexpected tuple type. Expected MpArray, but got: ${v.typeName()}"))
    })

  def valueUnsafe[A](implicit encoder: TupleEncoder[A]): ZIO[Any, Throwable, A] =
    value.map(_.require)

  // todo: return IO[???, A]
  /** use this method to get actual data after CRUD operations */
  def data[A](implicit
    encoder: TupleEncoder[A]
  ): Task[Attempt[Vector[A]]] = ZIO.effect(messagePack match {
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
  })

  def dataUnsafe[A](implicit encoder: TupleEncoder[A]): ZIO[Any, Throwable, Vector[A]] =
    data.map(_.require)
}
