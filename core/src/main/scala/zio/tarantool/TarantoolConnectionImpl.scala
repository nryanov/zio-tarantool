package zio.tarantool

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.AsynchronousSocketChannel
import java.lang.{Integer => JInteger, Long => JLong, Void => JVoid}
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

import zio._
import scodec.bits.ByteVector
import zio.interop.javaz._
import zio.tarantool.protocol.{AuthInfo, Key, MessagePackPacket, OperationCode}
import zio.tarantool.msgpack._
import zio.tarantool.msgpack.Implicits._
import zio.ZIO

import scala.util.control.NoStackTrace
import TarantoolConnectionImpl._
import zio.blocking.Blocking
import zio.clock.Clock
import zio.tarantool.TarantoolConnection.TarantoolConnectionService

import scala.collection.mutable

// todo: probably, we should use SocketChannel with Selector
final class TarantoolConnectionImpl(channel: AsynchronousSocketChannel) extends TarantoolConnection.Service {
  private val syncId = new AtomicLong(0)

  def connect(): ZIO[Any, IOException, Unit] =
    greeting().refineToOrDie[IOException].unit

  def connect(authInfo: AuthInfo): IO[IOException, Unit] = ???

  def createPacket(
    op: OperationCode,
    schemaId: Option[Long],
    body: MpMap
  ): MessagePackPacket = {
    val header = createHeader(op, syncId.incrementAndGet(), schemaId)
    MessagePackPacket(header, body)
  }

  def readPacket(): ZIO[Any, Throwable, MessagePackPacket] = for {
    buffer <- ZIO.effectTotal(ByteBuffer.allocate(MessageSizeLength))
    bytes <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(buffer, (), handler))
    vector = ByteVector(buffer.flip())
    size <- ZIO.effect(vector.decode().require.toNumber.toInt)
    messageBuffer <- ZIO.effectTotal(ByteBuffer.allocate(size))
    _ <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(messageBuffer, (), handler))
    message <- ZIO.effect(MessagePackPacket.messagePackPacketCodec.decodeValue(ByteVector(messageBuffer.flip()).toBitVector).require)
  } yield message

  override def writePacket(packet: MessagePackPacket): Task[Int] = for {
    buffer <- packet.toBuffer
    dataSent <- effectAsyncWithCompletionHandler[JInteger](handler => channel.write(buffer, (), handler))
  } yield dataSent

  private def greeting(): ZIO[Any, Throwable, String] = for {
    buffer <- ZIO(ByteBuffer.allocate(GreetingLength))
    _ <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(buffer, (), handler))
    firstLine <- ZIO(new String(buffer.array()))
    _ <- ZIO.effectTotal(buffer.clear())
    _ <- effectAsyncWithCompletionHandler[JInteger](handler => channel.read(buffer, (), handler))
    salt <- ZIO(new String(buffer.array()))
  } yield salt

  private def auth(): IO[IOException, Unit] = ???

  private def createHeader(op: OperationCode, syncId: Long, schemaId: Option[Long]): MpFixMap = {
    val header = mutable.Map[MessagePack, MessagePack]()
    header += MpPositiveFixInt(Key.Sync.value) -> MpUint32(syncId)
    header += MpPositiveFixInt(Key.Code.value) -> MpUint8(op.value)
    schemaId.foreach(id => header += MpPositiveFixInt(Key.Sync.value) -> MpUint32(id))
    MpFixMap(header.toMap)
  }

}

object TarantoolConnectionImpl {
  private val GreetingLength = 64
  private val MessageSizeLength = 5

  // todo: add more info
  case object UnexpectedData extends RuntimeException("Unexpected data") with NoStackTrace

  def live(clientConfig: ClientConfig): ZLayer[Blocking with Clock, Throwable, TarantoolConnectionService] =
    ZLayer.fromEffect {
      ZIO
        .effect(AsynchronousSocketChannel.open())
        .tap { channel =>
          ZIO
            .effect(new InetSocketAddress(clientConfig.host, clientConfig.port))
            .flatMap(address => fromFutureJava(channel.connect(address)).timeout(Duration.ofSeconds(5))) // todo: move to config
        }
        .map(channel => new TarantoolConnectionImpl(channel))
        .tap(connection => connection.connect())
    }
}
