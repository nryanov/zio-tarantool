package zio.tarantool.api

sealed trait UpdateOperationValidationResult

object UpdateOperationValidationResult {
  case object Success extends UpdateOperationValidationResult

  final case class Error(reason: String) extends UpdateOperationValidationResult
}
