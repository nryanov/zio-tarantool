package zio.tarantool.core

import zio._
import zio.logging._
import zio.macros.accessible
import zio.internal.Executor
import zio.tarantool._
import zio.tarantool.TarantoolError.toIOError
import zio.tarantool.protocol.{MessagePackPacket, ResponseCode, ResponseType, TarantoolResponse}
import zio.tarantool.protocol.Implicits.{RichByteVector, RichMessagePack}
import java.nio.ByteBuffer
import java.nio.channels.spi.SelectorProvider
import java.nio.channels.{SelectionKey, Selector}

import scodec.bits.ByteVector
import zio.tarantool.codec.MessagePackPacketCodec
import zio.tarantool.core.DelayedQueue.DelayedQueue
import zio.tarantool.core.RequestHandler.RequestHandler
import zio.tarantool.core.TarantoolConnection.TarantoolConnection

@accessible[ResponseHandler.Service]
private[tarantool] object ResponseHandler {
  type ResponseHandler = Has[Service]

  trait Service extends Serializable {
    def start(): IO[TarantoolError.IOError, Fiber.Runtime[Throwable, Nothing]]

    def complete(packet: MessagePackPacket): IO[TarantoolError, Unit]

    def close(): IO[TarantoolError.IOError, Unit]
  }

  val live: ZLayer[
    TarantoolConnection with RequestHandler with DelayedQueue with Logging,
    TarantoolError.IOError,
    ResponseHandler
  ] =
    ZLayer.fromServicesManaged[
      TarantoolConnection.Service,
      RequestHandler.Service,
      DelayedQueue.Service,
      Logging,
      TarantoolError.IOError,
      Service
    ]((connection, requestHandler, delayedQueue) => make(connection, requestHandler, delayedQueue))

  def make(
    connection: TarantoolConnection.Service,
    requestHandler: RequestHandler.Service,
    delayedQueue: DelayedQueue.Service
  ): ZManaged[Logging, TarantoolError.IOError, Service] =
    ZManaged.make(
      for {
        logger <- ZIO.service[Logger[String]]
        live = new Live(
          logger,
          connection,
          requestHandler,
          delayedQueue,
          ExecutionContextManager.singleThreaded()
        )
        _ <- live.start()
      } yield live
    )(_.close().orDie)

  /* message pack packet response size length in bytes */
  private val MessageSizeLength = 5

  private[tarantool] class Live(
    logger: Logger[String],
    connection: TarantoolConnection.Service,
    requestHandler: RequestHandler.Service,
    delayedQueue: DelayedQueue.Service,
    ec: ExecutionContextManager
  ) extends Service {

    override def start(): IO[TarantoolError.IOError, Fiber.Runtime[Throwable, Nothing]] =
      ZIO
        .ifM(connection.isBlocking)(
          ZIO.fail(new IllegalArgumentException("Channel should be in non-blocking mode")),
          start0()
        )
        .refineOrDie(toIOError)

    override def close(): IO[TarantoolError.IOError, Unit] =
      (logger.debug("Close ResponseHandler") *> ec.shutdown()).refineOrDie(toIOError)

    private def start0(): ZIO[Any, TarantoolError.IOError, Fiber.Runtime[Throwable, Nothing]] = {
      val selector: Selector = SelectorProvider.provider.openSelector

      connection.registerSelector(selector, SelectionKey.OP_READ) *>
        read(selector).forever.lock(Executor.fromExecutionContext(1000)(ec.executionContext)).fork
    }

    private def read(
      selector: Selector
    ): ZIO[Any, Throwable, Unit] = for {
      buffer <- ZIO.effectTotal(ByteBuffer.allocate(MessageSizeLength))
      _ <- readBuffer(buffer, selector)
      vector = makeByteVector(buffer)
      size <- vector.decodeM().map(_.toNumber.toInt)
      messageBuffer: ByteBuffer <- ZIO.effectTotal(ByteBuffer.allocate(size))
      _ <- readBuffer(messageBuffer, selector)
      packet <- decodeToMessagePackPacket(makeByteVector(messageBuffer))
      _ <- complete(packet).tapError(err =>
        logger.error(s"Could not complete operation: ${err.getLocalizedMessage}")
      )
    } yield ()

    private def decodeToMessagePackPacket(
      vector: ByteVector
    ): IO[TarantoolError.CodecError, MessagePackPacket] = IO
      .effect(
        MessagePackPacketCodec.decodeValue(vector.toBitVector).require
      )
      .mapError(TarantoolError.CodecError)

    private def readBuffer(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] = {
      var total: Int = 0

      for {
        read <- connection.read(buffer).tap(r => ZIO.effectTotal(total += r))
        _ <- ZIO
          .fail(
            TarantoolError.MessagePackPacketReadError("Error while reading message pack packet")
          )
          .when(read < 0)
        _ <- readViaSelector(buffer, selector)
          .tap(r => ZIO.effectTotal(total += r))
          .when(buffer.remaining() > 0)
      } yield total
    }

    private def readViaSelector(buffer: ByteBuffer, selector: Selector): ZIO[Any, Throwable, Int] =
      for {
        _ <- ZIO.effect(selector.select())
        read <- connection.read(buffer)
        total <-
          if (buffer.remaining() > 0) readViaSelector(buffer, selector).map(_ + read)
          else ZIO.succeed(read)
      } yield total

    private def makeByteVector(buffer: ByteBuffer): ByteVector = {
      buffer.flip()
      ByteVector.view(buffer)
    }

    override def complete(packet: MessagePackPacket): IO[TarantoolError, Unit] =
      for {
        syncId <- MessagePackPacket.extractSyncId(packet)
        _ <- logger.debug(s"Complete operation with id: $syncId")
        schemaId <- MessagePackPacket.extractSchemaId(packet)
        code <- MessagePackPacket.extractCode(packet)
        _ <- completeByCode(code, syncId, schemaId, packet).tapError(err =>
          requestHandler.fail(syncId, err.getLocalizedMessage)
        )
      } yield ()

    private def completeByCode(
      code: Long,
      syncId: Long,
      schemaId: Long,
      packet: MessagePackPacket
    ) = code match {
      case ResponseCode.Success.value            => completeSucceeded(schemaId, syncId, packet)
      case ResponseCode.WrongSchemaVersion.value => delayedQueue.reschedule(syncId, schemaId)
      case _                                     => completeFailed(syncId, packet)
    }

    private def completeSucceeded(schemaId: Long, syncId: Long, packet: MessagePackPacket) = for {
      responseType <- MessagePackPacket.responseType(packet)
      _ <- responseType match {
        case ResponseType.DataResponse =>
          MessagePackPacket
            .extractData(packet)
            .flatMap(data => requestHandler.complete(syncId, TarantoolResponse(schemaId, data)))
        case ResponseType.SqlResponse =>
          MessagePackPacket
            .extractSql(packet)
            .flatMap(data => requestHandler.complete(syncId, TarantoolResponse(schemaId, data)))
        case ResponseType.ErrorResponse =>
          IO.fail(TarantoolError.OperationException("Unexpected error in packet with SUCCEED_CODE"))
            .zipRight(completeFailed(syncId, packet))
      }
    } yield ()

    private def completeFailed(syncId: Long, packet: MessagePackPacket) =
      MessagePackPacket.extractError(packet).flatMap(error => requestHandler.fail(syncId, error))
  }

}
