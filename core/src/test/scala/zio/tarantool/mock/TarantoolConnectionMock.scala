package zio.tarantool.mock

import zio.tarantool.core._
import zio.test.mock.mockable

@mockable[TarantoolConnection.Service]
object TarantoolConnectionMock
