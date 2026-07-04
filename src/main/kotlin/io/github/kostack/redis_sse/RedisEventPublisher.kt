package io.github.kostack.redis_sse

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.stream.RecordId
import tools.jackson.databind.ObjectMapper
import java.nio.ByteBuffer

class RedisEventPublisher(
  private val factory: ReactiveRedisConnectionFactory,
  private val objectMapper: ObjectMapper,
  private val properties: SseProperties
) {
  suspend fun publish(
    channel: String,
    type: String,
    payload: Map<String, Any>
  ) {
    val streamKey = "${properties.streamKeyPrefix}:$channel"

    val body: Map<ByteBuffer, ByteBuffer> =
      mapOf(
        bytes(SseKeys.RECORD_FIELD_CHANNEL) to bytes(channel),
        bytes(SseKeys.RECORD_FIELD_TYPE) to bytes(type),
        bytes(SseKeys.RECORD_FIELD_PAYLOAD) to bytes(objectMapper.writeValueAsString(payload))
      )

    factory.reactiveConnection
      .streamCommands()
      .xAdd(bytes(streamKey), body)
      .map(RecordId::getValue)
      .awaitSingle()
  }

  private fun bytes(value: String): ByteBuffer = RedisStreamUtils.toBytes(value)
}
