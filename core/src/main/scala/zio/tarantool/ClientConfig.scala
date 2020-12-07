package zio.tarantool

final case class ClientConfig(host: String, port: Int)

object ClientConfig {
  def apply(host: String, port: Int): ClientConfig = new ClientConfig(host, port)
}
