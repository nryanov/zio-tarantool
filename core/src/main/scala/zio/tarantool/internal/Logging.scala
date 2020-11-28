package zio.tarantool.internal

import org.slf4j.{Logger, LoggerFactory}
import zio.{Task, ZIO}

private[tarantool] trait Logging { self =>
  val logger: Logger = LoggerFactory.getLogger(self.getClass)

  def trace(msg: String): Task[Unit] = ZIO.effect(logger.trace(msg))

  def debug(msg: String): Task[Unit] = ZIO.effect(logger.debug(msg))

  def info(msg: String): Task[Unit] = ZIO.effect(logger.info(msg))

  def warn(msg: String): Task[Unit] = ZIO.effect(logger.warn(msg))

  def error(msg: String): Task[Unit] = ZIO.effect(logger.error(msg))
}
