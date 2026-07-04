package io.github.kostack.redis_sse

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "redis-sse")
data class SseProperties(
  var streamKeyPrefix: String = "app:sse-events",
  val readCount: Long = 100,
  val blockTimeoutInSeconds: Long = 10,
  val retryInSeconds: Long = 3,
  val heartbeatIntervalInSeconds: Long = 15
)
