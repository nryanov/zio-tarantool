package zio.tarantool.mock

import zio.tarantool.core.RequestHandler
import zio.test.mock.mockable

@mockable[RequestHandler.Service]
object RequestHandlerMock
