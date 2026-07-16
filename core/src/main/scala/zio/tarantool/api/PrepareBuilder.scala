package zio.tarantool.api

import zio.{Promise, ZIO}
import zio.tarantool.TarantoolClient.TarantoolClient
import zio.tarantool.protocol.TarantoolResponse
import zio.tarantool.TarantoolError

final case class PrepareBuilder private[api] (
  private val target: Option[BuiltRequest.PrepareTarget] = None
) {
  def sql(sql: String): PrepareBuilder = copy(target = Some(BuiltRequest.PrepareTarget.Sql(sql)))

  def statementId(statementId: Int): PrepareBuilder =
    copy(target = Some(BuiltRequest.PrepareTarget.StatementId(statementId)))

  def run: ZIO[TarantoolClient, TarantoolError, Promise[TarantoolError, TarantoolResponse]] =
    for {
      target <- BuilderOps.require(target, "sql or statementId")
      response <- BuilderOps.run(BuiltRequest.Prepare(target))
    } yield response
}

object PrepareBuilder {
  def apply(): PrepareBuilder = new PrepareBuilder()
}
