package zio.tarantool.internal

import zio._
import zio.stream._
import zio.clock.Clock
import zio.tarantool.protocol.{
  MessagePackPacket,
  RequestCode,
  ResponseCode,
  TarantoolOperation,
  TarantoolRequest,
  TarantoolRequestBody
}
import zio.tarantool.{AuthInfo, TarantoolConfig, TarantoolError}
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.Base64

import zio.tarantool.internal.RequestHandler.RequestHandler
import zio.tarantool.internal.SyncIdProvider.SyncIdProvider

private[tarantool] object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def sendRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    private[tarantool] def forceSendRequest(request: TarantoolRequest): IO[TarantoolError, Unit]

    def receive(): Stream[TarantoolError, MessagePackPacket]
  }

  def sendRequest(
    request: TarantoolRequest
  ): ZIO[TarantoolConnection, TarantoolError, TarantoolOperation] =
    ZIO.accessM[TarantoolConnection](_.get.sendRequest(request))

  private[tarantool] def forceSendRequest(
    request: TarantoolRequest
  ): ZIO[TarantoolConnection, TarantoolError, Unit] =
    ZIO.accessM[TarantoolConnection](_.get.forceSendRequest(request))

  def receive(): ZStream[TarantoolConnection, TarantoolError, MessagePackPacket] =
    ZStream.access[TarantoolConnection](_.get.receive()).flatten

  val live: ZLayer[Clock with SyncIdProvider with RequestHandler with Has[
    TarantoolConfig
  ], TarantoolError, Has[Service]] =
    ZLayer.fromServicesManaged[
      TarantoolConfig,
      SyncIdProvider.Service,
      RequestHandler.Service,
      Clock,
      TarantoolError,
      Service
    ]((cfg, syncId, requestHandler) => make(cfg, syncId, requestHandler))

  def make(
    config: TarantoolConfig,
    syncIdProvider: SyncIdProvider.Service,
    requestHandler: RequestHandler.Service
  ): ZManaged[Clock, TarantoolError, Service] =
    for {
      openChannel <- AsyncSocketChannelProvider.connect(config)
      requestQueue <- Queue.bounded[ByteBuffer](config.clientConfig.requestQueueSize).toManaged_
      live = new Live(openChannel.channel, requestQueue, requestHandler)

      _ <- config.authInfo match {
        case None           => IO.unit.toManaged_
        case Some(authInfo) => auth(authInfo, openChannel.salt, live, syncIdProvider).toManaged_
      }

      _ <- live.run.forkManaged
    } yield live

  private[tarantool] class Live(
    channel: AsyncSocketChannelProvider,
    requestQueue: Queue[ByteBuffer],
    requestHandler: RequestHandler.Service
  ) extends TarantoolConnection.Service {

    override def sendRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation] =
      requestHandler.submitRequest(request).flatMap { operation =>
        TarantoolRequest
          .createPacket(request)
          .flatMap(packet => MessagePackPacket.toBuffer(packet).flatMap(buffer => requestQueue.offer(buffer)))
          .as(operation)
          .tapError(_ => requestHandler.fail(operation.request.syncId, "Error happened while sending request", 0))
      }

    override private[tarantool] def forceSendRequest(
      request: TarantoolRequest
    ): IO[TarantoolError, Unit] =
      TarantoolRequest
        .createPacket(request)
        .flatMap(packet =>
          MessagePackPacket
            .toBuffer(packet)
            .flatMap(buffer => channel.write(Chunk.fromByteBuffer(buffer)).mapError(TarantoolError.IOError))
        )

    // used by ResponseHandler fiber
    val receive: ZStream[Any, TarantoolError, MessagePackPacket] =
      channel.read.transduce(ByteStream.decoder).mapError(TarantoolError.InternalError)

    val run: ZIO[Any, TarantoolError, Unit] = send.forever.retryWhile(_ => true).unit

    private def send: IO[TarantoolError, Unit] =
      requestQueue.take.flatMap { request =>
        channel.write(Chunk.fromByteBuffer(request)).mapError(TarantoolError.IOError)
      }
  }

  private def auth(
    authInfo: AuthInfo,
    salt: Array[Byte],
    openedConnection: TarantoolConnection.Service,
    syncIdProvider: SyncIdProvider.Service
  ): ZIO[Any, TarantoolError, Unit] = for {
    syncId <- syncIdProvider.syncId()
    authRequest <- createAuthRequest(authInfo, salt, syncId).mapError(err => TarantoolError.InternalError(err))
    _ <- openedConnection.forceSendRequest(authRequest)
    responseOpt <- openedConnection.receive().take(1).runHead
    response <- ZIO.fromOption(responseOpt).orElseFail(TarantoolError.ProtocolError("Something went wrong during auth"))
    code <- MessagePackPacket.extractCode(response)
    _ <- ZIO.when(code != ResponseCode.Success)(
      MessagePackPacket.extractError(response).flatMap(error => ZIO.fail(TarantoolError.AuthError(error, code)))
    )
  } yield ()

  private def createAuthRequest(
    authInfo: AuthInfo,
    encodedSalt: Array[Byte],
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

      val body =
        TarantoolRequestBody.authBody(authInfo.username, "chap-sha1", auth1)

      TarantoolRequest(RequestCode.Auth, syncId, body)
    }
}
