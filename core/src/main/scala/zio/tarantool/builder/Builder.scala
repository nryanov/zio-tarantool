package zio.tarantool.builder

trait Builder[A] {
  def build(): A
}
