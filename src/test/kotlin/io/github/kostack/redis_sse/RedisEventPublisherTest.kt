package io.github.kostack.redis_sse

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.ReactiveRedisConnection
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.ReactiveStreamCommands
import org.springframework.data.redis.connection.stream.RecordId
import reactor.core.publisher.Mono
import tools.jackson.databind.ObjectMapper
import java.nio.ByteBuffer

class RedisEventPublisherTest {
  private val factory = mockk<ReactiveRedisConnectionFactory>()
  private val connection = mockk<ReactiveRedisConnection>()
  private val streamCommands = mockk<ReactiveStreamCommands>()
  private val objectMapper = mockk<ObjectMapper>()
  private val properties = SseProperties(streamKeyPrefix = "test:sse-events")

  private val publisher = RedisEventPublisher(factory, objectMapper, properties)

  @Test
  fun `publishes event to configured stream with serialized payload`() =
    runTest {
      val capturedKey = mutableListOf<ByteBuffer>()
      val capturedBody = mutableListOf<Map<ByteBuffer, ByteBuffer>>()
      val payload = mapOf<String, Any>("message" to "hello", "count" to 2)

      every { factory.reactiveConnection } returns connection
      every { connection.streamCommands() } returns streamCommands
      every { objectMapper.writeValueAsString(payload) } returns """{"message":"hello","count":2}"""
      every {
        streamCommands.xAdd(capture(capturedKey), capture(capturedBody))
      } returns Mono.just(RecordId.of("1700000000000-0"))

      publisher.publish("orders", "created", payload)

      assertThat(capturedKey.single().asString()).isEqualTo("test:sse-events:orders")
      assertThat(capturedBody.single().asStringMap())
        .containsExactlyInAnyOrderEntriesOf(
          mapOf(
            SseKeys.RECORD_FIELD_CHANNEL to "orders",
            SseKeys.RECORD_FIELD_TYPE to "created",
            SseKeys.RECORD_FIELD_PAYLOAD to """{"message":"hello","count":2}"""
          )
        )

      verify(exactly = 1) { objectMapper.writeValueAsString(payload) }
      verify(exactly = 1) { streamCommands.xAdd(any<ByteBuffer>(), any<Map<ByteBuffer, ByteBuffer>>()) }
    }

  private fun Map<ByteBuffer, ByteBuffer>.asStringMap(): Map<String, String> =
    entries.associate { (key, value) -> key.asString() to value.asString() }

  private fun ByteBuffer.asString(): String = RedisStreamUtils.fromBytes(this)
}
