package io.github.kostack.redis_sse

import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate

class RedisPurger(
  private val template: ReactiveRedisTemplate<String, String>,
  private val properties: SseProperties
) {
  suspend fun purge(channel: String) {
    val streamKey = "${properties.streamKeyPrefix}:$channel"
    template.delete(streamKey).awaitSingleOrNull()
  }
}
