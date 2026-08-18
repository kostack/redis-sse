package io.github.kostack.redis_sse

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Test
import org.springframework.data.redis.core.ReactiveRedisTemplate
import reactor.core.publisher.Mono

class RedisPurgerTest {
  private val template = mockk<ReactiveRedisTemplate<String, String>>()
  private val properties = SseProperties(streamKeyPrefix = "test:sse-events")

  private val purger = RedisPurger(template, properties)

  @Test
  fun `deletes configured stream for channel`() =
    runTest {
      every { template.delete("test:sse-events:orders") } returns Mono.just(1)

      purger.purge("orders")

      verify(exactly = 1) { template.delete("test:sse-events:orders") }
    }
}
