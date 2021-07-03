package zio.tarantool.core

import java.io.{EOFException, IOException}
import java.net.{InetSocketAddress, SocketAddress, StandardSocketOptions}
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, Channel, CompletionHandler}

import zio.logging.{Logger, Logging}
import zio.{Chunk, IO, Managed, UIO, ZIO, ZManaged}
import zio.stream.ZStream
import zio.tarantool.{TarantoolConfig, TarantoolError}
import AsyncSocketChannelProvider._

private[tarantool] class AsyncSocketChannelProvider(
  readBuffer: ByteBuffer,
  writeBuffer: ByteBuffer,
  channel: AsynchronousSocketChannel
) {
  val read: ZStream[Any, IOException, Byte] =
    ZStream.repeatEffectChunkOption {
      val receive =
        for {
          _ <- IO.effectTotal(readBuffer.clear())
          _ <- completeWith[Integer](channel)(channel.read(readBuffer, null, _))
            .filterOrFail(_ >= 0)(new EOFException())
          chunk <- IO.effectTotal {
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
    IO.when(chunk.nonEmpty) {
      IO.effectSuspendTotal {
        writeBuffer.clear()
        val (c, remainder) = chunk.splitAt(writeBuffer.capacity())
        writeBuffer.put(c.toArray)
        writeBuffer.flip()

        completeWith[Integer](channel)(channel.write(writeBuffer, null, _))
          .repeatWhile(_ => writeBuffer.hasRemaining)
          .zipRight(write(remainder))
      }
    }
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
  private val GreetingLength = 128
  private val ProtocolVersionLength = 64
  private val SaltLength = 44

  final case class OpenChannel(version: String, salt: String, channel: AsyncSocketChannelProvider)

  def connect(
    cfg: TarantoolConfig
  ): ZManaged[Logging, TarantoolError.IOError, OpenChannel] =
    (for {
      address <- UIO(
        new InetSocketAddress(cfg.connectionConfig.host, cfg.connectionConfig.port)
      ).toManaged_
      makeBuffer = IO.effectTotal(ByteBuffer.allocateDirect(1024))
      readBuffer <- makeBuffer.toManaged_
      writeBuffer <- makeBuffer.toManaged_
      channel <- openChannel(address)

      provider = new AsyncSocketChannelProvider(readBuffer, writeBuffer, channel)
      greeting <- provider.read.take(GreetingLength).runCollect.toManaged_

      (version, salt) = greeting.toArray.splitAt(ProtocolVersionLength)

    } yield OpenChannel(new String(version), new String(salt.take(SaltLength)), provider))
      .mapError(TarantoolError.IOError)

  def openChannel(
    address: SocketAddress
  ): ZManaged[Logging, IOException, AsynchronousSocketChannel] =
    Managed.fromAutoCloseable {
      for {
        logger <- ZIO.service[Logger[String]]
        channel <- IO.effect {
          val channel = AsynchronousSocketChannel.open()
          channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.box(true))
          channel.setOption(StandardSocketOptions.TCP_NODELAY, Boolean.box(true))
          channel
        }
        _ <- completeWith[Void](channel)(channel.connect(address, null, _))
        _ <- logger.info("Connected to the tarantool server.")
      } yield channel
    }.refineToOrDie[IOException]

  def completeWith[A](
    channel: Channel
  )(op: CompletionHandler[A, Any] => Any): IO[IOException, A] =
    IO.effectAsyncInterrupt { register =>
      op(completionHandler(register))
      Left(IO.effect(channel.close()).ignore)
    }

  def completionHandler[A](register: IO[IOException, A] => Unit): CompletionHandler[A, Any] =
    new CompletionHandler[A, Any] {
      def completed(result: A, attachment: Any): Unit = register(IO.succeedNow(result))

      def failed(error: Throwable, attachment: Any): Unit =
        error match {
          case e: IOException => register(IO.fail(e))
          case _              => register(IO.die(error))
        }
    }
}
