package zio.tarantool.impl

import java.nio.channels.SocketChannel

import scala.concurrent.ExecutionContext

class BackgroundWriterLive(channel: SocketChannel, ec: ExecutionContext) {}
