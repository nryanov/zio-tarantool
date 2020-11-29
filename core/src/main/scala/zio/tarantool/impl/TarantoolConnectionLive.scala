package zio.tarantool.impl

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import zio.{ZIO, _}
import zio.tarantool.impl.TarantoolConnectionLive.TarantoolOperationException
import zio.tarantool.internal.Logging
import zio.tarantool.{BackgroundReader, BackgroundWriter, PacketManager, SocketChannelProvider, TarantoolConnection, TarantoolOperation}
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.protocol.Constants._
import zio.tarantool.protocol.{AuthInfo, Code, MessagePackPacket, OperationCode}

import scala.util.control.NoStackTrace

final class TarantoolConnectionLive(
  channel: SocketChannelProvider.Service,
  packetManager: PacketManager.Service,
  backgroundReader: BackgroundReader.Service,
  backgroundWriter: BackgroundWriter.Service
) extends TarantoolConnection.Service
    with Logging {

  private val syncId: AtomicLong = new AtomicLong(0)
  private val operationResultMap: ConcurrentHashMap[Long, TarantoolOperation] = new ConcurrentHashMap[Long, TarantoolOperation]()

  def connect(): ZIO[Any, Throwable, Unit] =
    greeting().flatMap(_ => channel.blockingMode(false)).flatMap(_ => backgroundReader.start(complete)).orDie.unit

  def connect(authInfo: AuthInfo): IO[Throwable, Unit] = ???

  override def send(op: OperationCode, body: Map[Long, MessagePack]): ZIO[Any, Throwable, TarantoolOperation] = {
    val id = syncId.incrementAndGet()
    for {
      packet <- packetManager.createPacket(op, id, None, body)
      buffer <- packetManager.toBuffer(packet)
      promise <- Promise.make[Throwable, MessagePack]
      operation = TarantoolOperation(id, promise)
      _ <- ZIO.effectTotal(operationResultMap.put(id, operation))
      dataSent <- backgroundWriter.write(buffer)
      _ <- debug(s"Operation with id: $id was sent")
    } yield operation
  }

  override def close(): ZIO[Any, Throwable, Unit] = ZIO.effect(channel.close())

  private def greeting(): ZIO[Any, Throwable, String] = for {
    buffer <- ZIO(ByteBuffer.allocate(GreetingLength))
    bytes <- channel.read(buffer)
    firstLine <- ZIO(new String(buffer.array()))
    _ <- info(firstLine)
    _ <- ZIO.effectTotal(buffer.clear())
    bytes <- channel.read(buffer)
    salt <- ZIO(new String(buffer.array()))
  } yield salt

  private def auth(): IO[IOException, Unit] = ???

  private def complete(packet: MessagePackPacket): ZIO[Any, Throwable, Unit] =
    for {
      syncId <- packetManager.extractSyncId(packet)
      _ <- debug(s"Complete operation with id: $syncId")
      operation <- ZIO.fromOption(Option(operationResultMap.get(syncId))).mapError(_ => new RuntimeException("Operation does not exist"))
      _ <- ZIO.ifM(packetManager.extractCode(packet).map(_ == Code.Success.value))(
        packetManager.extractData(packet).flatMap(data => operation.promise.succeed(data)),
        packetManager.extractError(packet).flatMap(error => operation.promise.fail(TarantoolOperationException(error)))
      )
      _ <- ZIO.effect(operationResultMap.remove(syncId))
      _ <- debug(operationResultMap.toString)
    } yield ()
}

object TarantoolConnectionLive {
  final case class TarantoolOperationException(reason: String) extends RuntimeException(reason) with NoStackTrace
}
