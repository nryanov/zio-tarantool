package zio.tarantool

import zio.UIO
import org.slf4j.{Logger, LoggerFactory}

private[tarantool] trait Logging { self =>
  val logger: Logger = LoggerFactory.getLogger(self.getClass)

  def trace(msg: String): UIO[Unit] = UIO.effectTotal(logger.trace(msg))

  def debug(msg: String): UIO[Unit] = UIO.effectTotal(logger.debug(msg))

  def info(msg: String): UIO[Unit] = UIO.effectTotal(logger.info(msg))

  def warn(msg: String): UIO[Unit] = UIO.effectTotal(logger.warn(msg))

  def error(msg: String): UIO[Unit] = UIO.effectTotal(logger.error(msg))

  def error(msg: String, cause: Throwable): UIO[Unit] = UIO.effectTotal(logger.error(msg, cause))
}
