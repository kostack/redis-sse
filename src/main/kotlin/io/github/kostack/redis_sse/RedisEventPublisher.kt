package io.github.kostack.redis_sse

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.stream.RecordId
import tools.jackson.databind.ObjectMapper
import java.nio.ByteBuffer
import java.time.Duration

class RedisEventPublisher(
  private val factory: ReactiveRedisConnectionFactory,
  private val objectMapper: ObjectMapper,
  private val properties: SseProperties
) {
  suspend fun publish(
    channel: String,
    type: String,
    payload: Map<String, Any>,
    ttl: Long? = null
  ) {
    val streamKey = "${properties.streamKeyPrefix}:$channel"

    val body: Map<ByteBuffer, ByteBuffer> =
      mapOf(
        bytes(SseKeys.RECORD_FIELD_CHANNEL) to bytes(channel),
        bytes(SseKeys.RECORD_FIELD_TYPE) to bytes(type),
        bytes(SseKeys.RECORD_FIELD_PAYLOAD) to bytes(objectMapper.writeValueAsString(payload))
      )

    val connection = factory.reactiveConnection
    val streamKeyBytes = bytes(streamKey)

    connection
      .streamCommands()
      .xAdd(streamKeyBytes, body)
      .map(RecordId::getValue)
      .awaitSingle()

    if (ttl != null) {
      connection
        .keyCommands()
        .expire(streamKeyBytes, Duration.ofSeconds(ttl))
        .awaitSingle()
    }
  }

  private fun bytes(value: String): ByteBuffer = RedisStreamUtils.toBytes(value)
}
