package zio.tarantool.protocol

import zio.test._
import zio.test.Assertion._
import zio.test.environment.testEnvironment
import zio.tarantool.TarantoolError
import zio.tarantool.msgpack.{Encoder, MessagePack, MpPositiveFixInt}

import java.nio.ByteBuffer

object MessagePackPacketSpec extends DefaultRunnableSpec {
  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("MessagePackPacket")(
      testM("should convert to ByteBuffer") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(OperationCode.Ping.value)
          )
        )
        val result = MessagePackPacket.toBuffer(packet)
        assertM(result)(isSubtype[ByteBuffer](anything))
      },
      testM("should extract code") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(
              ResponseCode.Success.value
            )
          )
        )
        val result = MessagePackPacket.extractCode(packet)
        assertM(result)(equalTo(ResponseCode.Success.value.toLong))
      },
      testM("should fail with ProtocolError when trying to extract not existing code") {
        val packet =
          MessagePackPacket(Map(FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1)))
        val result = MessagePackPacket.extractCode(packet)
        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.ProtocolError(s"Packet has no Code in header (${packet.header})")
            )
          )
        )
      },
      testM("should extract error code") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(0x8123)
          )
        )
        val result = MessagePackPacket.extractCode(packet)
        assertM(result)(equalTo(0x0123.toLong))
      },
      testM("should fail with ProtocolError when trying to extract incorrect error code") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(0x1234)
          )
        )
        val result = MessagePackPacket.extractCode(packet)
        assertM(result.run)(
          fails(
            equalTo(TarantoolError.ProtocolError(s"Code ${0x1234} does not follow 0x8XXX format"))
          )
        )
      },
      testM("should extract data") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(0x0)
          ),
          Some(
            Map(
              FieldKey.Data.value -> Encoder[Long].encodeUnsafe(123)
            )
          )
        )
        val result = MessagePackPacket.extractData(packet)
        assertM(result)(equalTo(MpPositiveFixInt(123)))
      },
      testM("should fail with ProtocolError when trying to extract not existing data") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(0x0)
          ),
          Some(Map.empty[Long, MessagePack])
        )
        val result = MessagePackPacket.extractData(packet)
        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.ProtocolError(
                s"Packet has no ${FieldKey.Data} value in body part ${packet.body}"
              )
            )
          )
        )
      },
      testM("should extract syncId") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(0x8123)
          )
        )
        val result = MessagePackPacket.extractSyncId(packet)
        assertM(result)(equalTo(1.toLong))
      },
      testM("should fail with ProtocolError when trying to extract not existing syncId") {
        val packet = MessagePackPacket(Map.empty[Long, MessagePack])
        val result = MessagePackPacket.extractSyncId(packet)
        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.ProtocolError(s"Packet has no SyncId in header (${packet.header})")
            )
          )
        )
      },
      testM("should extract schemaId") {
        val packet = MessagePackPacket(
          Map(
            FieldKey.Sync.value -> Encoder[Long].encodeUnsafe(1),
            FieldKey.Code.value -> Encoder[Long].encodeUnsafe(0x8123),
            FieldKey.SchemaId.value -> Encoder[Long].encodeUnsafe(2)
          )
        )
        val result = MessagePackPacket.extractSchemaId(packet)
        assertM(result)(equalTo(2.toLong))
      },
      testM("should fail with ProtocolError when trying to extract not existing schemaId") {
        val packet = MessagePackPacket(Map.empty[Long, MessagePack])
        val result = MessagePackPacket.extractSchemaId(packet)
        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.ProtocolError(s"Packet has no SchemaId in header (${packet.header})")
            )
          )
        )
      }
    ).provideCustomLayerShared(testEnvironment)
}
