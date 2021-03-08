package zio.tarantool.mock

import zio.tarantool.core.DelayedQueue
import zio.test.mock.mockable

@mockable[DelayedQueue.Service]
object DelayedQueueMock
