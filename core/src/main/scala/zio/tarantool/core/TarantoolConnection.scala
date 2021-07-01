package zio.tarantool.core

import zio._
import zio.stream._
import zio.logging._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.protocol.{
  MessagePackPacket,
  RequestCode,
  TarantoolRequest,
  TarantoolRequestBody
}
import zio.tarantool.{AuthInfo, TarantoolConfig, TarantoolError}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.Base64

import scodec.bits.ByteVector
import zio.tarantool.core.SyncIdProvider.SyncIdProvider

@accessible[TarantoolConnection.Service]
private[tarantool] object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def sendRequest(packet: MessagePackPacket): IO[TarantoolError, Boolean]

    private[tarantool] def forceSendRequest(packet: MessagePackPacket): IO[TarantoolError, Unit]

    def receive(): Stream[TarantoolError, MessagePackPacket]
  }

  val live: ZLayer[Logging with Clock with SyncIdProvider with Has[
    TarantoolConfig
  ], TarantoolError, Has[Service]] =
    ZLayer.fromServiceManaged[
      TarantoolConfig,
      Logging with Clock with SyncIdProvider,
      TarantoolError,
      Service
    ](cfg => ZIO.service[SyncIdProvider.Service].toManaged_.flatMap(service => make(cfg, service)))

  def make(
    config: TarantoolConfig,
    syncIdProvider: SyncIdProvider.Service
  ): ZManaged[Logging with Clock, TarantoolError, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      openChannel <- AsyncSocketChannelProvider.connect(config)
      //todo: configured queue size
      requestQueue <- Queue.bounded[ByteBuffer](64).toManaged_
      _ <- logger.info(s"Protocol version: ${openChannel.version}").toManaged_
      live = new Live(logger, openChannel.channel, requestQueue)

      _ <- config.authInfo match {
        case None           => IO.unit.toManaged_
        case Some(authInfo) => auth(authInfo, openChannel.salt, live, syncIdProvider).toManaged_
      }

      _ <- live.run.forkManaged
    } yield live

  private[tarantool] class Live(
    logger: Logger[String],
    channel: AsyncSocketChannelProvider,
    requestQueue: Queue[ByteBuffer]
  ) extends TarantoolConnection.Service {

    override def sendRequest(packet: MessagePackPacket): IO[TarantoolError, Boolean] =
      MessagePackPacket.toBuffer(packet).flatMap(buffer => requestQueue.offer(buffer))

    override private[tarantool] def forceSendRequest(
      packet: MessagePackPacket
    ): IO[TarantoolError, Unit] =
      MessagePackPacket
        .toBuffer(packet)
        .flatMap(buffer =>
          channel.write(Chunk.fromByteBuffer(buffer)).mapError(TarantoolError.IOError)
        )

    // used by ResponseHandler fiber
    val receive: ZStream[Any, TarantoolError, MessagePackPacket] =
      channel.read.transduce(ByteStream.decoder).mapError(TarantoolError.InternalError)

    val run: ZIO[Any, TarantoolError, Unit] = send.forever
      .tapError(e => logger.warn(s"Reconnecting due to error: $e"))
      .retryWhile(_ => true)
      .unit

    private def send: IO[TarantoolError, Unit] =
      requestQueue.take.flatMap { request =>
        channel.write(Chunk.fromByteBuffer(request)).mapError(TarantoolError.IOError)
      }
  }

  private def auth(
    authInfo: AuthInfo,
    salt: String,
    openedConnection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ): ZIO[Logging, TarantoolError, Unit] = for {
    syncId <- syncIdProvider.syncId()
    authRequest <- createAuthRequest(authInfo, salt, syncId).mapError(err =>
      TarantoolError.InternalError(err)
    )
    packet <- TarantoolRequest.createPacket(authRequest)
    _ <- openedConnection.forceSendRequest(packet)
    responseOpt <- openedConnection.receive().take(1).runHead
    response <- ZIO
      .fromOption(responseOpt)
      .orElseFail(TarantoolError.ProtocolError("Something went wrong during auth"))
    code <- MessagePackPacket.extractCode(response)
    _ <- ZIO.when(code != 0)(
      MessagePackPacket
        .extractError(response)
        .flatMap(error => ZIO.fail(TarantoolError.AuthError(error)))
    )
  } yield ()

  private def createAuthRequest(
    authInfo: AuthInfo,
    encodedSalt: String,
    syncId: Long
  ): ZIO[Any, Throwable, TarantoolRequest] =
    IO.effect {
      val sha1: MessageDigest = MessageDigest.getInstance("SHA-1")
      val auth1: Array[Byte] = sha1.digest(authInfo.password.getBytes)
      val auth2: Array[Byte] = sha1.digest(auth1)
      val salt: Array[Byte] = Base64.getDecoder.decode(encodedSalt)
      sha1.update(salt, 0, 20)
      sha1.update(auth2)
      val scramble: Array[Byte] = sha1.digest()

      (0 until 20).foreach { i =>
        auth1.update(i, auth1(i).^(scramble(i)).toByte)
      }

      val body = TarantoolRequestBody.authBody(
        authInfo.username,
        Vector(ByteVector.view("chap-sha1".getBytes), ByteVector.view(auth1))
      )

      TarantoolRequest(RequestCode.Auth, syncId, None, body)
    }
}
