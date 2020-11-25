package zio.tarantool.impl

import java.io.IOException
import java.lang.{Integer => JInteger}
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.util.concurrent.atomic.AtomicLong

import scodec.bits.ByteVector
import zio.{ZIO, _}
import zio.interop.javaz._
import zio.tarantool.impl.TarantoolConnectionLive.TarantoolOperationException
import zio.tarantool.{PacketManager, TarantoolConnection}
import zio.tarantool.msgpack.Implicits._
import zio.tarantool.msgpack.{MessagePack, MpMap}
import zio.tarantool.protocol.Constants._
import zio.tarantool.protocol.{AuthInfo, Code, MessagePackPacket, OperationCode}

import scala.util.control.NoStackTrace

// todo: probably, we should use SocketChannel with Selector
final class TarantoolConnectionLive(channel: AsynchronousSocketChannel, packetManager: PacketManager.Service)
    extends TarantoolConnection.Service {
  private val syncId = new AtomicLong(0)

  def connect(): ZIO[Any, IOException, Unit] =
    greeting().refineToOrDie[IOException].unit

  def connect(authInfo: AuthInfo): IO[IOException, Unit] = ???

  override def read(): ZIO[Any, Throwable, MessagePack] = for {
    buffer <- ZIO.effectTotal(ByteBuffer.allocate(MessageSizeLength))
    bytes <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(buffer, (), handler))
    vector = ByteVector(buffer.flip())
    size <- ZIO.effect(vector.decode().require.toNumber.toInt)
    messageBuffer <- ZIO.effectTotal(ByteBuffer.allocate(size))
    _ <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(messageBuffer, (), handler))
    packet <- ZIO.effect(MessagePackPacket.messagePackPacketCodec.decodeValue(ByteVector(messageBuffer.flip()).toBitVector).require)
    data <- getDataOrFail(packet)
  } yield data

  override def write(op: OperationCode, body: MpMap): ZIO[Any, Throwable, Int] = for {
    packet <- packetManager.createPacket(op, syncId.incrementAndGet(), None, body)
    buffer <- packetManager.toBuffer(packet)
    dataSent <- effectAsyncWithCompletionHandler[JInteger](handler => channel.write(buffer, (), handler))
  } yield dataSent

  private def getDataOrFail(packet: MessagePackPacket): ZIO[Any, Throwable, MessagePack] =
    ZIO.ifM(packetManager.extractCode(packet).map(_ == Code.Success.value))(
      packetManager.extractData(packet),
      packetManager.extractError(packet).flatMap(error => ZIO.fail(TarantoolOperationException(error)))
    )

  private def greeting(): ZIO[Any, Throwable, String] = for {
    buffer <- ZIO(ByteBuffer.allocate(GreetingLength))
    _ <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(buffer, (), handler))
    firstLine <- ZIO(new String(buffer.array()))
    _ <- ZIO.effectTotal(buffer.clear())
    _ <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(buffer, (), handler))
    salt <- ZIO(new String(buffer.array()))
  } yield salt

  private def auth(): IO[IOException, Unit] = ???
}

object TarantoolConnectionLive {
  final case class TarantoolOperationException(reason: String) extends RuntimeException(reason) with NoStackTrace
}
