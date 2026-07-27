package zio.tarantool

import zio.tarantool.TarantoolConfig.DefaultClientConfig

final case class AuthInfo(username: String, password: String)

final case class ConnectionConfig(
  host: String,
  port: Int,
  connectionTimeoutMillis: Int = 3000,
  retryTimeoutMillis: Int = 1000,
  retries: Int = 3
)

final case class ClientConfig(
  schemaRequestTimeoutMillis: Int = 10000,
  requestQueueSize: Int = 64,
  useSchemaMetaCache: Boolean = true,
  reconnectEnabled: Boolean = true,
  reconnectRetries: Int = 3,
  reconnectIntervalMillis: Int = 1000,
  reconnectWaitTimeoutMillis: Int = 10000
)

final case class TarantoolConfig(
  connectionConfig: ConnectionConfig,
  clientConfig: ClientConfig = DefaultClientConfig,
  authInfo: Option[AuthInfo] = None
)

object TarantoolConfig {
  val DefaultClientConfig: ClientConfig = ClientConfig()

  def apply(
    connectionConfig: ConnectionConfig,
    clientConfig: ClientConfig,
    authInfo: Option[AuthInfo]
  ): TarantoolConfig = new TarantoolConfig(connectionConfig, clientConfig, authInfo)

  def apply(host: String, port: Int) = new TarantoolConfig(
    ConnectionConfig(host, port)
  )

  def apply(host: String, port: Int, authInfo: AuthInfo) = new TarantoolConfig(
    connectionConfig = ConnectionConfig(host, port),
    authInfo = Some(authInfo)
  )

  def apply() = new TarantoolConfig(ConnectionConfig("localhost", 3301))
}
