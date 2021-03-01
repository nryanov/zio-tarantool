package zio.tarantool.mock.custom

import java.nio.ByteBuffer
import java.nio.channels.Selector

import zio.clock.Clock
import zio.tarantool.TarantoolError
import zio.tarantool.core.SocketChannelProvider
import zio.tarantool.core.SocketChannelProvider.SocketChannelProvider
import zio.test.mock
import zio.test.mock.Mock
import zio._

class SocketChannelProviderMock(clock: Clock, writeDelayMs: Int = 0, readDelayMs: Int = 0)
    extends Mock[SocketChannelProvider] {
  object IsBlocking extends Effect[Unit, Nothing, Boolean]
  object RegisterSelector extends Effect[(Selector, Int), TarantoolError.IOError, Unit]
  object Close extends Effect[Unit, TarantoolError.IOError, Unit]
  object Read extends Effect[ByteBuffer, TarantoolError.IOError, Int]
  object WriteFully extends Effect[ByteBuffer, TarantoolError.IOError, Option[Int]]
  object BlockingMode extends Effect[Boolean, TarantoolError.IOError, Unit]

  val compose: URLayer[Has[mock.Proxy], SocketChannelProvider] =
    ZLayer.fromService { proxy =>
      new SocketChannelProvider.Service {
        override def isBlocking(): UIO[Boolean] = proxy(IsBlocking)

        override def registerSelector(
          selector: Selector,
          selectionKey: Int
        ): IO[TarantoolError.IOError, Unit] = proxy(RegisterSelector, selector, selectionKey)

        override def close(): IO[TarantoolError.IOError, Unit] = proxy(Close)

        override def read(buffer: ByteBuffer): IO[TarantoolError.IOError, Int] = proxy(Read, buffer)

        override def writeFully(buffer: ByteBuffer): IO[TarantoolError, Option[Int]] =
          proxy(WriteFully, buffer)

        override def blockingMode(flag: Boolean): IO[TarantoolError.IOError, Unit] =
          proxy(BlockingMode, flag)
      }
    }
}
