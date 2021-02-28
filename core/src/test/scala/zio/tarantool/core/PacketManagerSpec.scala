package zio.tarantool.core

import zio.test._
import zio.test.environment.testEnvironment

object PacketManagerSpec extends DefaultRunnableSpec {
  val packetManager = PacketManager.live

  override def spec: ZSpec[_root_.zio.test.environment.TestEnvironment, Any] =
    suite("Packet manager")().provideCustomLayerShared(testEnvironment ++ packetManager)
}
