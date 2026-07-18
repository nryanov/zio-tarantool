package zio.tarantool.internal

import _root_.zio._
import _root_.zio.stream._
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

private[tarantool] object TarantoolConnection {

  trait Service extends Serializable {
    def sendRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    private[tarantool] def forceSendRequest(request: TarantoolRequest): IO[TarantoolError, Unit]

    def receive(): Stream[TarantoolError, MessagePackPacket]
  }

  def sendRequest(
    request: TarantoolRequest
  ): ZIO[Service, TarantoolError, TarantoolOperation] =
    ZIO.serviceWithZIO(_.sendRequest(request))

  private[tarantool] def forceSendRequest(
    request: TarantoolRequest
  ): ZIO[Service, TarantoolError, Unit] =
    ZIO.serviceWithZIO(_.forceSendRequest(request))

  def receive(): ZStream[Service, TarantoolError, MessagePackPacket] =
    ZStream.serviceWithStream(_.receive())

  val live: ZLayer[
    Clock with SyncIdProvider.Service with RequestHandler.Service with TarantoolConfig,
    TarantoolError,
    Service
  ] =
    ZLayer.scoped {
      for {
        cfg <- ZIO.service[TarantoolConfig]
        syncId <- ZIO.service[SyncIdProvider.Service]
        requestHandler <- ZIO.service[RequestHandler.Service]
        service <- make(cfg, syncId, requestHandler)
      } yield service
    }

  def make(
    config: TarantoolConfig,
    syncIdProvider: SyncIdProvider.Service,
    requestHandler: RequestHandler.Service
  ): ZIO[Scope with Clock, TarantoolError, Service] =
    for {
      openChannel <- AsyncSocketChannelProvider.connect(config)
      requestQueue <- ZIO.acquireRelease(Queue.bounded[ByteBuffer](config.clientConfig.requestQueueSize))(_.shutdown)
      live = new Live(openChannel.channel, requestQueue, requestHandler)

      _ <- config.authInfo match {
        case None           => ZIO.unit
        case Some(authInfo) => auth(authInfo, openChannel.salt, live, syncIdProvider)
      }

      _ <- live.run.forkScoped
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
    override def receive(): ZStream[Any, TarantoolError, MessagePackPacket] =
      channel.read.via(ByteStream.decoder).mapError(TarantoolError.InternalError)

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
    ZIO.attempt {
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
