package zio.tarantool.core

import zio._
import zio.stream._
import zio.logging._
import zio.clock.Clock
import zio.macros.accessible
import zio.tarantool.protocol.MessagePackPacket
import zio.tarantool.{TarantoolConfig, TarantoolError}
import java.nio.ByteBuffer

@accessible[TarantoolConnection.Service]
object TarantoolConnection {

  type TarantoolConnection = Has[Service]

  trait Service extends Serializable {
    def sendRequest(packet: MessagePackPacket): IO[TarantoolError, Boolean]

    def receive(): Stream[TarantoolError, MessagePackPacket]
  }

  val live: ZLayer[Logging with Clock with Has[TarantoolConfig], TarantoolError, Has[Service]] =
    ZLayer.fromServiceManaged[TarantoolConfig, Logging with Clock, TarantoolError, Service](cfg =>
      make(cfg)
    )

  def make(config: TarantoolConfig): ZManaged[Logging with Clock, TarantoolError, Service] =
    for {
      logger <- ZIO.service[Logger[String]].toManaged_
      channelProvider <- AsyncSocketChannelProvider.connect(config)
      //todo: configured queue size
      requestQueue <- Queue.bounded[ByteBuffer](64).toManaged_
      live = new Live(logger, channelProvider, requestQueue)
      _ <- live.run.forkManaged
    } yield live

  private[tarantool] class Live(
    logger: Logger[String],
    channel: AsyncSocketChannelProvider,
    requestQueue: Queue[ByteBuffer]
  ) extends TarantoolConnection.Service {

    override def sendRequest(packet: MessagePackPacket): IO[TarantoolError, Boolean] =
      MessagePackPacket.toBuffer(packet).flatMap(buffer => requestQueue.offer(buffer))

    def receive(): ZStream[Any, TarantoolError, MessagePackPacket] =
      channel.read.transduce(ByteStream.decoder).mapError(TarantoolError.InternalError)

    val run: ZIO[Any, TarantoolError, Unit] = send.forever
      .tapError(e => logger.warn(s"Reconnecting due to error: $e"))
      .retryWhile(_ => true)
      .unit

    private def send: IO[TarantoolError, Unit] =
      requestQueue.take.flatMap { request =>
        channel.write(Chunk.fromByteBuffer(request)).mapError(TarantoolError.IOError)
//          .tapBoth(
//            e => IO.foreach_(reqs)(_.promise.fail(e)),
//            _ => IO.foreach_(reqs)(req => resQueue.offer(req.promise))
//          )
      }
  }
}
