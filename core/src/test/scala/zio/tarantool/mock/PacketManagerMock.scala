package zio.tarantool.mock

import zio.tarantool.internal.PacketManager
import zio.test.mock.mockable

@mockable[PacketManager.Service]
object PacketManagerMock
