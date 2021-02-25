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
  SchemaMetaManager,
  SocketChannelProvider
}
import zio.tarantool.{Logging, TarantoolConfig, TarantoolConnection, TarantoolError}
import zio.tarantool.msgpack.MessagePack
import zio.tarantool.protocol.Constants._
import zio.tarantool.protocol.{
  Code,
  MessagePackPacket,
  OperationCode,
  TarantoolOperation,
  TarantoolRequest,
  TarantoolResponse
}

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
  private val awaitingRequestMap: ConcurrentHashMap[Long, TarantoolOperation] =
    new ConcurrentHashMap[Long, TarantoolOperation]()

  def connect(): ZIO[Any, Throwable, Unit] =
    greeting()
      .zipRight(channel.blockingMode(false))
      .zipRight(backgroundReader.start(complete))
      .zipRight(backgroundWriter.start())
      .orDie
      .unit

  override def send(
    op: OperationCode,
    body: Map[Long, MessagePack]
  ): IO[TarantoolError, TarantoolOperation] = {
    val id = syncId.incrementAndGet()
    for {
      // todo: schemaId instead of None
      packet <- packetManager.createPacket(op, id, None, body)
      buffer <- packetManager.toBuffer(packet)
      request = TarantoolRequest(op, packet.header, packet.body)
      response <- Promise.make[Throwable, TarantoolResponse]
      operation = TarantoolOperation(request, response)
      _ <- ZIO.effectTotal(awaitingRequestMap.put(id, operation))
      dataSent <- backgroundWriter.write(buffer)
      _ <- debug(s"Operation with id: $id was sent (bytes: $dataSent)")
    } yield operation
  }

  private def greeting(): ZIO[Any, Throwable, String] = for {
    buffer <- ZIO(ByteBuffer.allocate(GreetingLength))
    _ <- channel.read(buffer)
    firstLine = new String(buffer.array())
    _ <- info(firstLine)
    _ <- ZIO.effectTotal(buffer.clear())
    _ <- channel.read(buffer)
    salt = new String(buffer.array())
  } yield salt

  // todo: auth
  private def auth(): IO[IOException, Unit] = ???

  private def complete(packet: MessagePackPacket): ZIO[Any, Throwable, Unit] =
    for {
      syncId <- packetManager.extractSyncId(packet)
      schemaId <- packetManager.extractSchemaId(packet)
      _ <- debug(s"Complete operation with id: $syncId")
      operation <- ZIO
        .fromOption(Option(awaitingRequestMap.get(syncId)))
        .orElseFail(new RuntimeException("Operation does not exist"))
      // todo: check code Success, IncorrectSchema, Error, Sql
      _ <- ZIO.ifM(packetManager.extractCode(packet).map(_ == Code.Success.value))(
        packetManager
          .extractData(packet)
          .flatMap(data => operation.response.succeed(TarantoolResponse(schemaId, data))),
        packetManager
          .extractError(packet)
          .flatMap(error => operation.response.fail(TarantoolOperationException(error)))
      )
      _ <- ZIO.effectTotal(awaitingRequestMap.remove(syncId))
    } yield ()
}

object TarantoolConnectionLive {
  final case class TarantoolOperationException(reason: String)
      extends RuntimeException(reason)
      with NoStackTrace
}
