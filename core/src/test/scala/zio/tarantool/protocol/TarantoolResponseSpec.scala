package zio.tarantool.protocol

import org.msgpack.value.Value
import org.msgpack.value.impl._
import _root_.zio._
import zio.tarantool.TarantoolError
import zio.tarantool.TarantoolError.{CodecError, EmptyResultSet}
import zio.tarantool.codec.TupleEncoder
import zio.tarantool.data.TestTuple
import zio.tarantool.protocol.Implicits._
import zio.tarantool.protocol.TarantoolResponse.{TarantoolDataResponse, TarantoolEvalResponse}
import _root_.zio.test._
import _root_.zio.test.Assertion._
import _root_.zio.test.TestAspect.sequential

object TarantoolResponseSpec extends ZIOSpecDefault {
  private val tarantoolEvalResponseResultSet =
    test("TarantoolEvalResponse should return resultSet") {
      val tuple = TestTuple("f1", 2, 3L)

      for {
        encodedTuple <- encodeTuple(tuple)
        response = TarantoolEvalResponse(encodedTuple)
        resultSet <- response.resultSet[TestTuple]
      } yield assert(resultSet)(equalTo(Vector(tuple)))
    }

  private val tarantoolEvalResponseEmptyResultSet =
    test("TarantoolEvalResponse should return empty resultSet") {
      val response = TarantoolEvalResponse(new ImmutableArrayValueImpl(Array.empty))
      for {
        resultSet <- response.resultSet[TestTuple]
      } yield assert(resultSet)(equalTo(Vector.empty))
    }

  private val tarantoolEvalResponseHeadOption =
    test("TarantoolEvalResponse should return head option") {
      val tuple = TestTuple("f1", 2, 3L)

      for {
        encodedTuple <- encodeTuple(tuple)
        response = TarantoolEvalResponse(encodedTuple)
        resultSet <- response.headOption[TestTuple]
      } yield assert(resultSet)(isSome(equalTo(tuple)))
    }

  private val tarantoolEvalResponseHead =
    test("TarantoolEvalResponse should return head") {
      val tuple = TestTuple("f1", 2, 3L)

      for {
        encodedTuple <- encodeTuple(tuple)
        response = TarantoolEvalResponse(encodedTuple)
        resultSet <- response.head[TestTuple]
      } yield assert(resultSet)(equalTo(tuple))
    }

  private val tarantoolEvalResponseHeadEmptyResultSet =
    test("TarantoolEvalResponse should fail when head is called on an empty result set") {
      val response = TarantoolEvalResponse(new ImmutableArrayValueImpl(Array.empty))
      for {
        resultSet <- response.head[TestTuple].exit
      } yield assert(resultSet)(fails(equalTo(EmptyResultSet)))
    }

  private val tarantoolEvalResponseFailOnIncorrectMessagePack =
    test("TarantoolEvalResponse should fail on incorrect message pack type") {
      val response =
        TarantoolEvalResponse(new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1))))
      for {
        resultSet <- response.resultSet[TestTuple].exit
      } yield assert(resultSet)(fails(isSubtype[CodecError](anything)))
    }

  private val tarantoolDataResponseResultSet =
    test("TarantoolDataResponse should return resultSet") {
      val tuple = TestTuple("f1", 2, 3L)

      for {
        encodedTuple <- encodeTuple(tuple)
        response = TarantoolDataResponse(new ImmutableArrayValueImpl(Array(encodedTuple)))
        resultSet <- response.resultSet[TestTuple]
      } yield assert(resultSet)(equalTo(Vector(tuple)))
    }

  private val tarantoolDataResponseEmptyResultSet =
    test("TarantoolDataResponse should return empty resultSet") {
      val response = TarantoolDataResponse(new ImmutableArrayValueImpl(Array.empty))
      for {
        resultSet <- response.resultSet[TestTuple]
      } yield assert(resultSet)(equalTo(Vector.empty))
    }

  private val tarantoolDataResponseHeadOption =
    test("TarantoolDataResponse should return head option") {
      val tuple = TestTuple("f1", 2, 3L)

      for {
        encodedTuple <- encodeTuple(tuple)
        response = TarantoolDataResponse(new ImmutableArrayValueImpl(Array(encodedTuple)))
        resultSet <- response.headOption[TestTuple]
      } yield assert(resultSet)(isSome(equalTo(tuple)))
    }

  private val tarantoolDataResponseHead =
    test("TarantoolEvalResponse should return head") {
      val tuple = TestTuple("f1", 2, 3L)

      for {
        encodedTuple <- encodeTuple(tuple)
        response = TarantoolDataResponse(new ImmutableArrayValueImpl(Array(encodedTuple)))
        resultSet <- response.head[TestTuple]
      } yield assert(resultSet)(equalTo(tuple))
    }

  private val tarantoolDataResponseHeadEmptyResultSet =
    test("TarantoolDataResponse should fail when head is called on an empty result set") {
      val response = TarantoolDataResponse(new ImmutableArrayValueImpl(Array.empty))
      for {
        resultSet <- response.head[TestTuple].exit
      } yield assert(resultSet)(fails(equalTo(EmptyResultSet)))
    }

  private val tarantoolDataResponseFailOnIncorrectMessagePack =
    test("TarantoolDataResponse should fail on incorrect message pack type") {
      val response =
        TarantoolDataResponse(
          new ImmutableArrayValueImpl(
            Array(new ImmutableArrayValueImpl(Array(new ImmutableLongValueImpl(1))))
          )
        )
      for {
        resultSet <- response.resultSet[TestTuple].exit
      } yield assert(resultSet)(fails(isSubtype[CodecError](anything)))
    }

  override def spec: Spec[TestEnvironment, Any] =
    suite("TarantoolResponse")(
      tarantoolEvalResponseResultSet,
      tarantoolEvalResponseEmptyResultSet,
      tarantoolEvalResponseHeadOption,
      tarantoolEvalResponseHead,
      tarantoolEvalResponseHeadEmptyResultSet,
      tarantoolEvalResponseFailOnIncorrectMessagePack,
      tarantoolDataResponseResultSet,
      tarantoolDataResponseEmptyResultSet,
      tarantoolDataResponseHeadOption,
      tarantoolDataResponseHead,
      tarantoolDataResponseHeadEmptyResultSet,
      tarantoolDataResponseFailOnIncorrectMessagePack
    ) @@ sequential

  private def encodeTuple(tuple: TestTuple)(implicit
    encoder: TupleEncoder[TestTuple]
  ): IO[TarantoolError.CodecError, Value] = encoder.encodeM(tuple)
}
