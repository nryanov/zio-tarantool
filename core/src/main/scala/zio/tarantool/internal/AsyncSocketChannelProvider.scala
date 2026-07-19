package zio.tarantool.internal

import java.nio.ByteBuffer
import java.io.{EOFException, IOException}
import java.net.{ConnectException, InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.channels.{AsynchronousSocketChannel, Channel, CompletionHandler}

import _root_.zio._
import _root_.zio.stream.ZStream
import zio.tarantool.{TarantoolConfig, TarantoolError}
import AsyncSocketChannelProvider._

private[tarantool] class AsyncSocketChannelProvider(
  readBuffer: ByteBuffer,
  writeBuffer: ByteBuffer,
  channel: AsynchronousSocketChannel
) {
  val read: ZStream[Any, IOException, Byte] =
    ZStream.repeatZIOChunkOption {
      val receive =
        for {
          _ <- ZIO.succeed(readBuffer.clear())
          _ <- completeWith[Integer](channel)(channel.read(readBuffer, null, _))
            .filterOrFail(_ >= 0)(new EOFException())
          chunk <- ZIO.succeed {
            readBuffer.flip()
            val count = readBuffer.remaining()
            val array = Array.ofDim[Byte](count)
            readBuffer.get(array)
            Chunk.fromArray(array)
          }
        } yield chunk

      receive.mapError {
        case _: EOFException => None
        case e: IOException  => Some(e)
      }
    }

  def write(chunk: Chunk[Byte]): IO[IOException, Unit] =
    ZIO
      .when(chunk.nonEmpty) {
        ZIO.suspendSucceed {
          writeBuffer.clear()
          val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
          writeBuffer.put(c.toArray)
          writeBuffer.flip()

          completeWith[Integer](channel)(channel.write(writeBuffer, null, _))
            .repeatWhile(_ => writeBuffer.hasRemaining)
            .zipRight(write(remainder))
        }
      }
      .unit
}

private[tarantool] object AsyncSocketChannelProvider {
  /*
  When a client connects to the server instance,
  the instance responds with a 128-byte text greeting message, not in MsgPack format:
  64-byte Greeting text line 1
  64-byte Greeting text line 2

  44-byte base64-encoded salt
  20-byte NULL
   */
  private val GreetingLength: Long = 128
  private val ProtocolVersionLength: Int = 64
  private val SaltLength: Int = 44

  final case class OpenChannel(
    version: String,
    salt: Array[Byte],
    channel: AsyncSocketChannelProvider
  )

  def connect(
    cfg: TarantoolConfig
  ): ZIO[Scope with Clock, TarantoolError, OpenChannel] =
    (for {
      address <- ZIO.succeed(new InetSocketAddress(cfg.connectionConfig.host, cfg.connectionConfig.port))
      makeBuffer = ZIO.succeed(ByteBuffer.allocateDirect(1024))
      readBuffer <- makeBuffer
      writeBuffer <- makeBuffer
      channel <- openChannel(address)
        .timeout(cfg.connectionConfig.connectionTimeoutMillis.millis)
        .retry(
          Schedule.recurs(cfg.connectionConfig.retries) && Schedule
            .spaced(cfg.connectionConfig.retryTimeoutMillis.millis)
        )
        .flatMap(opt => ZIO.fromEither(opt.toRight(new ConnectException("Connection time out"))))

      provider = new AsyncSocketChannelProvider(readBuffer, writeBuffer, channel)
      greeting <- provider.read.take(GreetingLength).runCollect

      (version, salt) = greeting.toArray.splitAt(ProtocolVersionLength)

    } yield OpenChannel(new String(version), salt.take(SaltLength), provider)).mapError(TarantoolError.IOError)

  def openChannel(
    address: SocketAddress
  ): ZIO[Scope, IOException, AsynchronousSocketChannel] =
    ZIO.fromAutoCloseable {
      for {
        channel <- ZIO.attempt {
          val channel = AsynchronousSocketChannel.open()
          channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
          channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
          channel
        }
        _ <- completeWith[Void](channel)(channel.connect(address, null, _))
      } yield channel
    }.refineToOrDie[IOException]

  def completeWith[A](
    channel: Channel
  )(op: CompletionHandler[A, Any] => Any): IO[IOException, A] =
    ZIO.asyncInterrupt { register =>
      op(completionHandler(register))
      Left(ZIO.attempt(channel.close()).ignore)
    }

  def completionHandler[A](register: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, attachment: Any): Unit = register(ZIO.succeed(result))

      def failed(error: Throwable, attachment: Any): Unit =
        error match {
          case e: IOException => register(ZIO.fail(e))
          case _              => register(ZIO.die(error))
        }
    }
}
