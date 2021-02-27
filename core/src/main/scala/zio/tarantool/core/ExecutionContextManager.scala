package zio.tarantool.core

import java.util.concurrent.{ExecutorService, Executors}

import zio.{Task, ZIO}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

final class ExecutionContextManager(es: ExecutorService) {
  val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(es)

  def shutdown(): Task[Unit] = ZIO.effect(es.shutdown())
}

object ExecutionContextManager {
  def singleThreaded(): ExecutionContextManager = new ExecutionContextManager(
    Executors.newSingleThreadExecutor()
  )
}
