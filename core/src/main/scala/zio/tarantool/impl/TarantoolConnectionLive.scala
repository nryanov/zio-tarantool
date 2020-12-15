package zio.tarantool.impl

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import zio.{ZIO, _}
import zio.tarantool.impl.TarantoolConnectionLive.TarantoolOperationException
import zio.tarantool.internal.{
  BackgroundReader,
  BackgroundWriter,
  PacketManager,
  SocketChannelProvider
}
import zio.tarantool.{Logging, TarantoolConfig, TarantoolConnection, TarantoolOperation}
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.protocol.Constants._
import zio.tarantool.protocol.{Code, MessagePackPacket, OperationCode}

import scala.util.control.NoStackTrace

final class TarantoolConnectionLive(
  tarantoolConfig: TarantoolConfig,
  channel: SocketChannelProvider.Service,
  packetManager: PacketManager.Service,
  backgroundReader: BackgroundReader.Service,
  backgroundWriter: BackgroundWriter.Service
) extends TarantoolConnection.Service
    with Logging {

  // todo: Ref[Long]
  private val syncId: AtomicLong = new AtomicLong(0)
  private val operationResultMap: ConcurrentHashMap[Long, TarantoolOperation] =
    new ConcurrentHashMap[Long, TarantoolOperation]()

  def connect(): ZIO[Any, Throwable, Unit] =
    greeting()
      .flatMap(_ => channel.blockingMode(false))
      .flatMap(_ => backgroundReader.start(complete))
      .orDie
      .unit

  override def send(
    op: OperationCode,
    body: Map[Long, MessagePack]
  ): ZIO[Any, Throwable, TarantoolOperation] = {
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

  private def greeting(): ZIO[Any, Throwable, String] = for {
    buffer <- ZIO(ByteBuffer.allocate(GreetingLength))
    _ <- channel.read(buffer)
    firstLine <- ZIO(new String(buffer.array()))
    _ <- info(firstLine)
    _ <- ZIO.effectTotal(buffer.clear())
    _ <- channel.read(buffer)
    salt <- ZIO(new String(buffer.array()))
  } yield salt

  private def auth(): IO[IOException, Unit] = ???

  private def complete(packet: MessagePackPacket): ZIO[Any, Throwable, Unit] =
    for {
      syncId <- packetManager.extractSyncId(packet)
      _ <- debug(s"Complete operation with id: $syncId")
      operation <- ZIO
        .fromOption(Option(operationResultMap.get(syncId)))
        .mapError(_ => new RuntimeException("Operation does not exist"))
      _ <- ZIO.ifM(packetManager.extractCode(packet).map(_ == Code.Success.value))(
        packetManager.extractData(packet).flatMap(data => operation.promise.succeed(data)),
        packetManager
          .extractError(packet)
          .flatMap(error => operation.promise.fail(TarantoolOperationException(error)))
      )
      _ <- ZIO.effect(operationResultMap.remove(syncId))
    } yield ()
}

object TarantoolConnectionLive {
  final case class TarantoolOperationException(reason: String)
      extends RuntimeException(reason)
      with NoStackTrace
}
