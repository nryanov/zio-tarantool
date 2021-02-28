package zio.tarantool.mock

import zio.tarantool.core.PacketManager
import zio.test.mock.mockable

@mockable[PacketManager.Service]
object PacketManagerMock
