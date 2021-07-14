//package zio.tarantool.protocol
// fixme
//import zio._
//import zio.tarantool.TarantoolError
//import zio.tarantool.TarantoolError.{CodecError, EmptyResultSet}
//import zio.tarantool.codec.TupleEncoder
//import zio.tarantool.data.TestTuple
//import zio.tarantool.msgpack.{MessagePack, MpFixArray, MpPositiveFixInt}
//import zio.tarantool.protocol.Implicits._
//import zio.tarantool.protocol.TarantoolResponse.{TarantoolDataResponse, TarantoolEvalResponse}
//import zio.test._
//import zio.test.Assertion._
//import zio.test.TestAspect.sequential
//
//object TarantoolResponseSpec extends DefaultRunnableSpec {
//  private val tarantoolEvalResponseResultSet =
//    testM("TarantoolEvalResponse should return resultSet") {
//      val tuple = TestTuple("f1", 2, 3L)
//
//      for {
//        encodedTuple <- encodeTuple(tuple)
//        response = TarantoolEvalResponse(encodedTuple)
//        resultSet <- response.resultSet[TestTuple]
//      } yield assert(resultSet)(equalTo(Vector(tuple)))
//    }
//
//  private val tarantoolEvalResponseEmptyResultSet =
//    testM("TarantoolEvalResponse should return empty resultSet") {
//      val response = TarantoolEvalResponse(MpFixArray(Vector.empty))
//      for {
//        resultSet <- response.resultSet[TestTuple]
//      } yield assert(resultSet)(equalTo(Vector.empty))
//    }
//
//  private val tarantoolEvalResponseHeadOption =
//    testM("TarantoolEvalResponse should return head option") {
//      val tuple = TestTuple("f1", 2, 3L)
//
//      for {
//        encodedTuple <- encodeTuple(tuple)
//        response = TarantoolEvalResponse(encodedTuple)
//        resultSet <- response.headOption[TestTuple]
//      } yield assert(resultSet)(isSome(equalTo(tuple)))
//    }
//
//  private val tarantoolEvalResponseHead =
//    testM("TarantoolEvalResponse should return head") {
//      val tuple = TestTuple("f1", 2, 3L)
//
//      for {
//        encodedTuple <- encodeTuple(tuple)
//        response = TarantoolEvalResponse(encodedTuple)
//        resultSet <- response.head[TestTuple]
//      } yield assert(resultSet)(equalTo(tuple))
//    }
//
//  private val tarantoolEvalResponseHeadEmptyResultSet =
//    testM("TarantoolEvalResponse should fail when head is called on an empty result set") {
//      val response = TarantoolEvalResponse(MpFixArray(Vector.empty))
//      for {
//        resultSet <- response.head[TestTuple].run
//      } yield assert(resultSet)(fails(equalTo(EmptyResultSet)))
//    }
//
//  private val tarantoolEvalResponseFailOnIncorrectMessagePack =
//    testM("TarantoolEvalResponse should fail on incorrect message pack type") {
//      val response = TarantoolEvalResponse(MpFixArray(Vector(MpPositiveFixInt(1))))
//      for {
//        resultSet <- response.resultSet[TestTuple].run
//      } yield assert(resultSet)(fails(isSubtype[CodecError](anything)))
//    }
//
//  private val tarantoolDataResponseResultSet =
//    testM("TarantoolDataResponse should return resultSet") {
//      val tuple = TestTuple("f1", 2, 3L)
//
//      for {
//        encodedTuple <- encodeTuple(tuple)
//        response = TarantoolDataResponse(MpFixArray(Vector(encodedTuple)))
//        resultSet <- response.resultSet[TestTuple]
//      } yield assert(resultSet)(equalTo(Vector(tuple)))
//    }
//
//  private val tarantoolDataResponseEmptyResultSet =
//    testM("TarantoolDataResponse should return empty resultSet") {
//      val response = TarantoolDataResponse(MpFixArray(Vector.empty))
//      for {
//        resultSet <- response.resultSet[TestTuple]
//      } yield assert(resultSet)(equalTo(Vector.empty))
//    }
//
//  private val tarantoolDataResponseHeadOption =
//    testM("TarantoolDataResponse should return head option") {
//      val tuple = TestTuple("f1", 2, 3L)
//
//      for {
//        encodedTuple <- encodeTuple(tuple)
//        response = TarantoolDataResponse(MpFixArray(Vector(encodedTuple)))
//        resultSet <- response.headOption[TestTuple]
//      } yield assert(resultSet)(isSome(equalTo(tuple)))
//    }
//
//  private val tarantoolDataResponseHead =
//    testM("TarantoolEvalResponse should return head") {
//      val tuple = TestTuple("f1", 2, 3L)
//
//      for {
//        encodedTuple <- encodeTuple(tuple)
//        response = TarantoolDataResponse(MpFixArray(Vector(encodedTuple)))
//        resultSet <- response.head[TestTuple]
//      } yield assert(resultSet)(equalTo(tuple))
//    }
//
//  private val tarantoolDataResponseHeadEmptyResultSet =
//    testM("TarantoolDataResponse should fail when head is called on an empty result set") {
//      val response = TarantoolDataResponse(MpFixArray(Vector.empty))
//      for {
//        resultSet <- response.head[TestTuple].run
//      } yield assert(resultSet)(fails(equalTo(EmptyResultSet)))
//    }
//
//  private val tarantoolDataResponseFailOnIncorrectMessagePack =
//    testM("TarantoolDataResponse should fail on incorrect message pack type") {
//      val response =
//        TarantoolDataResponse(MpFixArray(Vector(MpFixArray(Vector(MpPositiveFixInt(1))))))
//      for {
//        resultSet <- response.resultSet[TestTuple].run
//      } yield assert(resultSet)(fails(isSubtype[CodecError](anything)))
//    }
//
//  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
//    suite("TarantoolResponse")(
//      tarantoolEvalResponseResultSet,
//      tarantoolEvalResponseEmptyResultSet,
//      tarantoolEvalResponseHeadOption,
//      tarantoolEvalResponseHead,
//      tarantoolEvalResponseHeadEmptyResultSet,
//      tarantoolEvalResponseFailOnIncorrectMessagePack,
//      tarantoolDataResponseResultSet,
//      tarantoolDataResponseEmptyResultSet,
//      tarantoolDataResponseHeadOption,
//      tarantoolDataResponseHead,
//      tarantoolDataResponseHeadEmptyResultSet,
//      tarantoolDataResponseFailOnIncorrectMessagePack
//    ) @@ sequential
//
//  private def encodeTuple(tuple: TestTuple)(implicit
//    encoder: TupleEncoder[TestTuple]
//  ): IO[TarantoolError.CodecError, MessagePack] = encoder.encodeM(tuple)
//}
