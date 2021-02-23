package zio.tarantool.internal

import java.nio.ByteBuffer

import zio.RIO
import zio.test._
import zio.test.Assertion._
import zio.test.environment.testEnvironment
import zio.tarantool.TarantoolError
import zio.tarantool.internal.PacketManager.PacketManager
import zio.tarantool.msgpack.{Encoder, MessagePack, MpPositiveFixInt}
import zio.tarantool.protocol.{Code, Key, MessagePackPacket, OperationCode}

object PacketManagerSpec extends DefaultRunnableSpec {
  val packetManager = PacketManager.live

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Packet manager")(
      testM("should create packet") {
        val expected = MessagePackPacket(
          Map(
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(OperationCode.Ping.value)
          )
        )
        val result: RIO[PacketManager, MessagePackPacket] =
          PacketManager.createPacket(OperationCode.Ping, 1, None, Map.empty)
        assertM(result)(equalTo(expected))
      },
      testM("should convert to ByteBuffer") {
        val packet = MessagePackPacket(
          Map(
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(OperationCode.Ping.value)
          )
        )
        val result = PacketManager.toBuffer(packet)
        assertM(result)(isSubtype[ByteBuffer](anything))
      },
      testM("should extract code") {
        val packet = MessagePackPacket(
          Map(
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(Code.Success.value)
          )
        )
        val result = PacketManager.extractCode(packet)
        assertM(result)(equalTo(Code.Success.value.toLong))
      },
      testM("should fail with ProtocolError when trying to extract not existing code") {
        val packet = MessagePackPacket(Map(Key.Sync.value -> Encoder[Long].encodeUnsafe(1)))
        val result = PacketManager.extractCode(packet)
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
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(0x8123)
          )
        )
        val result = PacketManager.extractCode(packet)
        assertM(result)(equalTo(0x0123.toLong))
      },
      testM("should fail with ProtocolError when trying to extract incorrect error code") {
        val packet = MessagePackPacket(
          Map(
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(0x1234)
          )
        )
        val result = PacketManager.extractCode(packet)
        assertM(result.run)(
          fails(
            equalTo(TarantoolError.ProtocolError(s"Code ${0x1234} does not follow 0x8XXX format"))
          )
        )
      },
      testM("should extract data") {
        val packet = MessagePackPacket(
          Map(
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(0x0)
          ),
          Some(
            Map(
              Key.Data.value -> Encoder[Long].encodeUnsafe(123)
            )
          )
        )
        val result = PacketManager.extractData(packet)
        assertM(result)(equalTo(MpPositiveFixInt(123)))
      },
      testM("should fail with ProtocolError when trying to extract not existing data") {
        val packet = MessagePackPacket(
          Map(
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(0x0)
          ),
          Some(Map.empty[Long, MessagePack])
        )
        val result = PacketManager.extractData(packet)
        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.ProtocolError(
                s"Packet has no ${Key.Data} value in body part ${packet.body}"
              )
            )
          )
        )
      },
      testM("should extract syncId") {
        val packet = MessagePackPacket(
          Map(
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(0x8123)
          )
        )
        val result = PacketManager.extractSyncId(packet)
        assertM(result)(equalTo(1.toLong))
      },
      testM("should fail with ProtocolError when trying to extract not existing syncId") {
        val packet = MessagePackPacket(Map.empty[Long, MessagePack])
        val result = PacketManager.extractSyncId(packet)
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
            Key.Sync.value -> Encoder[Long].encodeUnsafe(1),
            Key.Code.value -> Encoder[Long].encodeUnsafe(0x8123),
            Key.SchemaId.value -> Encoder[Long].encodeUnsafe(2)
          )
        )
        val result = PacketManager.extractSchemaId(packet)
        assertM(result)(equalTo(2.toLong))
      },
      testM("should fail with ProtocolError when trying to extract not existing schemaId") {
        val packet = MessagePackPacket(Map.empty[Long, MessagePack])
        val result = PacketManager.extractSchemaId(packet)
        assertM(result.run)(
          fails(
            equalTo(
              TarantoolError.ProtocolError(s"Packet has no SchemaId in header (${packet.header})")
            )
          )
        )
      }
    ).provideCustomLayerShared(testEnvironment ++ packetManager)
}
