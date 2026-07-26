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

  sealed trait ConnectionState extends Product with Serializable

  object ConnectionState {
    case object Connected extends ConnectionState
    case object Reconnecting extends ConnectionState
    final case class Failed(cause: TarantoolError) extends ConnectionState
  }

  trait Service extends Serializable {
    def sendRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation]

    private[tarantool] def forceSendRequest(request: TarantoolRequest): IO[TarantoolError, Unit]

    def receive(): Stream[TarantoolError, MessagePackPacket]

    def setAfterReconnect(hook: UIO[Unit]): UIO[Unit]

    /** Test/ops helper: close the current socket to trigger reconnect. */
    private[tarantool] def forceReconnect(): UIO[Unit]
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

  def setAfterReconnect(hook: UIO[Unit]): ZIO[Service, Nothing, Unit] =
    ZIO.serviceWithZIO(_.setAfterReconnect(hook))

  private[tarantool] def forceReconnect(): ZIO[Service, Nothing, Unit] =
    ZIO.serviceWithZIO(_.forceReconnect())

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
      channelRef <- Ref.make(openChannel.channel)
      stateRef <- Ref.make[ConnectionState](ConnectionState.Connected)
      gate <- Promise.make[TarantoolError, Unit]
      _ <- gate.succeed(())
      gateRef <- Ref.make(gate)
      afterReconnectRef <- Ref.make[UIO[Unit]](ZIO.unit)
      live = new Live(
        config,
        syncIdProvider,
        requestHandler,
        channelRef,
        stateRef,
        gateRef,
        requestQueue,
        afterReconnectRef
      )

      _ <- config.authInfo match {
        case None           => ZIO.unit
        case Some(authInfo) => live.authenticate(openChannel.channel, openChannel.salt, authInfo)
      }

      _ <- ZIO.addFinalizer(channelRef.get.flatMap(_.close()))
      _ <- live.writeLoop.forkScoped
      _ <- live.readLoop.forkScoped
    } yield live

  private[tarantool] class Live(
    config: TarantoolConfig,
    syncIdProvider: SyncIdProvider.Service,
    requestHandler: RequestHandler.Service,
    channelRef: Ref[AsyncSocketChannelProvider],
    stateRef: Ref[ConnectionState],
    gateRef: Ref[Promise[TarantoolError, Unit]],
    requestQueue: Queue[ByteBuffer],
    afterReconnectRef: Ref[UIO[Unit]]
  ) extends TarantoolConnection.Service {

    private val clientConfig = config.clientConfig

    override def setAfterReconnect(hook: UIO[Unit]): UIO[Unit] =
      afterReconnectRef.set(hook)

    override private[tarantool] def forceReconnect(): UIO[Unit] =
      channelRef.get.flatMap(_.close()) *> triggerReconnect("Forced reconnect")

    override def sendRequest(request: TarantoolRequest): IO[TarantoolError, TarantoolOperation] =
      awaitReady *>
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
      channelRef.get.flatMap(writeDirect(_, request))

    override def receive(): ZStream[Any, TarantoolError, MessagePackPacket] =
      ZStream.unwrap {
        channelRef.get.map { channel =>
          channel.read.via(ByteStream.decoder).mapError(e => TarantoolError.InternalError(e))
        }
      }

    private[tarantool] val writeLoop: ZIO[Clock, Nothing, Unit] =
      (awaitReady.ignore *>
        requestQueue.take.flatMap { buffer =>
          stateRef.get.flatMap {
            case ConnectionState.Connected =>
              channelRef.get.flatMap { channel =>
                channel
                  .write(Chunk.fromByteBuffer(buffer))
                  .mapError(TarantoolError.IOError)
                  .catchAll(err => triggerReconnect(Option(err.getMessage).getOrElse("write error")))
              }
            case _ =>
              ZIO.unit
          }
        }).forever

    private[tarantool] val readLoop: ZIO[Clock, Nothing, Unit] =
      (awaitReady.ignore *>
        stateRef.get.flatMap {
          case ConnectionState.Connected =>
            channelRef.get.flatMap { channel =>
              channel.read
                .via(ByteStream.decoder)
                .mapError(e => TarantoolError.InternalError(e): TarantoolError)
                .foreach(packet => ResponseHandler.completePacket(requestHandler, packet).catchAll(_ => ZIO.unit))
                .foldZIO(
                  err => triggerReconnect(Option(err.getMessage).getOrElse("read error")),
                  _ =>
                    stateRef.get.flatMap {
                      case ConnectionState.Connected =>
                        triggerReconnect("Connection closed")
                      case _ => ZIO.unit
                    }
                )
            }
          case _ =>
            ZIO.unit
        }).forever

    private def awaitReady: IO[TarantoolError, Unit] =
      stateRef.get.flatMap {
        case ConnectionState.Connected =>
          ZIO.unit
        case ConnectionState.Failed(cause) =>
          ZIO.fail(TarantoolError.ReconnectFailed(cause))
        case ConnectionState.Reconnecting =>
          for {
            gate <- gateRef.get
            _ <- gate.await
              .timeoutFail(
                TarantoolError.Timeout(
                  s"Timed out waiting for reconnect after ${clientConfig.reconnectWaitTimeoutMillis} ms"
                )
              )(clientConfig.reconnectWaitTimeoutMillis.millis)
              .provideLayer(ZLayer.succeed[Clock](Clock.ClockLive))
            _ <- awaitReady

          } yield ()
      }

    private def triggerReconnect(reason: String): UIO[Unit] =
      stateRef.modify {
        case ConnectionState.Connected => (true, ConnectionState.Reconnecting)
        case other                     => (false, other)
      }.flatMap {
        case true =>
          doReconnect(reason).provideLayer(ZLayer.succeed[Clock](Clock.ClockLive)).forkDaemon.unit
        case false => ZIO.unit
      }

    private def doReconnect(reason: String): URIO[Clock, Unit] = {
      val lost = TarantoolError.ConnectionLost(reason)

      for {
        gate <- Promise.make[TarantoolError, Unit]
        _ <- gateRef.set(gate)
        _ <- requestHandler.failAll(lost)
        _ <- drainQueue
        oldChannel <- channelRef.get
        _ <- oldChannel.close()
        result <- reconnectAttempts.either
        _ <- result match {
          case Right(open) =>
            for {
              _ <- channelRef.set(open.channel)
              _ <- stateRef.set(ConnectionState.Connected)
              _ <- gate.succeed(())
              hook <- afterReconnectRef.get
              _ <- hook
            } yield ()
          case Left(err) =>
            val failed = TarantoolError.ReconnectFailed(err)
            stateRef.set(ConnectionState.Failed(err)) *> gate.fail(failed).unit
        }
      } yield ()
    }

    private def reconnectAttempts: ZIO[Clock, TarantoolError, AsyncSocketChannelProvider.OpenChannel] = {
      val cfg = clientConfig
      if (!cfg.reconnectEnabled) {
        ZIO.fail(TarantoolError.ConnectionLost("Reconnect disabled"))
      } else if (cfg.reconnectRetries <= 0) {
        ZIO.fail(TarantoolError.ConnectionLost("Reconnect retries exhausted"))
      } else {
        def attempt: ZIO[Clock, TarantoolError, AsyncSocketChannelProvider.OpenChannel] =
          AsyncSocketChannelProvider.connectOnce(config).flatMap { open =>
            val authed = config.authInfo match {
              case None => ZIO.succeed(open)
              case Some(authInfo) =>
                authenticate(open.channel, open.salt, authInfo).as(open)
            }
            authed.tapError(_ => open.channel.close())
          }

        def loop(remaining: Int): ZIO[Clock, TarantoolError, AsyncSocketChannelProvider.OpenChannel] =
          attempt.catchAll { err =>
            if (remaining <= 1) ZIO.fail(err)
            else ZIO.sleep(cfg.reconnectIntervalMillis.millis) *> loop(remaining - 1)
          }

        loop(cfg.reconnectRetries)
      }
    }

    private def drainQueue: UIO[Unit] =
      requestQueue.poll.flatMap {
        case Some(_) => drainQueue
        case None    => ZIO.unit
      }

    private[tarantool] def authenticate(
      channel: AsyncSocketChannelProvider,
      salt: Array[Byte],
      authInfo: AuthInfo
    ): IO[TarantoolError, Unit] =
      for {
        syncId <- syncIdProvider.syncId()
        authRequest <- createAuthRequest(authInfo, salt, syncId).mapError(err => TarantoolError.InternalError(err))
        _ <- writeDirect(channel, authRequest)
        response <- readOne(channel)
        code <- MessagePackPacket.extractCode(response)
        _ <- ZIO.when(code != ResponseCode.Success)(
          MessagePackPacket.extractError(response).flatMap(error => ZIO.fail(TarantoolError.AuthError(error, code)))
        )
      } yield ()

    private def writeDirect(
      channel: AsyncSocketChannelProvider,
      request: TarantoolRequest
    ): IO[TarantoolError, Unit] =
      TarantoolRequest
        .createPacket(request)
        .flatMap(packet =>
          MessagePackPacket
            .toBuffer(packet)
            .flatMap(buffer => channel.write(Chunk.fromByteBuffer(buffer)).mapError(TarantoolError.IOError))
        )

    private def readOne(channel: AsyncSocketChannelProvider): IO[TarantoolError, MessagePackPacket] =
      channel.read
        .via(ByteStream.decoder)
        .mapError(e => TarantoolError.InternalError(e))
        .take(1)
        .runHead
        .flatMap(opt =>
          ZIO.fromOption(opt).orElseFail(TarantoolError.ProtocolError("Something went wrong during auth"))
        )
  }

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
