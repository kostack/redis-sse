package io.github.kostack.redis_sse

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.ReactiveRedisConnection
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.ReactiveScriptingCommands
import org.springframework.data.redis.connection.ReturnType
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import tools.jackson.databind.ObjectMapper
import java.nio.ByteBuffer
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RedisEventPublisherTest {
  private val factory = mockk<ReactiveRedisConnectionFactory>()
  private val connection = mockk<ReactiveRedisConnection>()
  private val scriptingCommands = mockk<ReactiveScriptingCommands>()
  private val objectMapper = mockk<ObjectMapper>()
  private val properties = SseProperties(streamKeyPrefix = "test:sse-events")
  private val capturedArgs = mutableListOf<List<String>>()

  private val publisher = RedisEventPublisher(factory, objectMapper, properties)

  @BeforeEach
  fun setUp() {
    every { factory.reactiveConnection } returns connection
    every { connection.scriptingCommands() } returns scriptingCommands
    every { connection.closeLater() } returns Mono.empty()
    every { objectMapper.writeValueAsString(any()) } returns """{"message":"hello"}"""
    every {
      scriptingCommands.eval<Any>(any(), ReturnType.VALUE, 1, *anyVararg())
    } answers {
      @Suppress("UNCHECKED_CAST")
      val keysAndArgs = (args[3] as Array<ByteBuffer>).map { RedisStreamUtils.fromBytes(it) }
      capturedArgs += keysAndArgs
      Flux.just(RedisStreamUtils.toBytes("1700000000000-0"))
    }
  }

  @Test
  fun `publishes event to configured stream and returns record id`() =
    runTest {
      val payload = mapOf<String, Any?>("message" to "hello", "missing" to null)

      val id = publisher.publish("orders", "created", payload)

      assertThat(id).isEqualTo("1700000000000-0")
      assertThat(capturedArgs.single())
        .containsExactly(
          "test:sse-events:orders",
          "",
          "",
          SseKeys.RECORD_FIELD_CHANNEL,
          "orders",
          SseKeys.RECORD_FIELD_TYPE,
          "created",
          SseKeys.RECORD_FIELD_PAYLOAD,
          """{"message":"hello"}"""
        )
      verify(exactly = 1) { objectMapper.writeValueAsString(payload) }
      verify(exactly = 1) { connection.closeLater() }
    }

  @Test
  fun `passes ttl in milliseconds and max length to the script`() =
    runTest {
      val trimmingPublisher =
        RedisEventPublisher(factory, objectMapper, properties.copy(maxLength = 1000))

      trimmingPublisher.publish("orders", "created", emptyMap(), ttl = 60.seconds)

      assertThat(capturedArgs.single().subList(0, 3))
        .containsExactly("test:sse-events:orders", "60000", "1000")
    }

  @Test
  fun `rejects non-positive ttl without touching redis`() =
    runTest {
      for (ttl in listOf(Duration.ZERO, (-5).seconds, 0.5.milliseconds)) {
        assertFailsWith<IllegalArgumentException> {
          publisher.publish("orders", "created", emptyMap(), ttl = ttl)
        }
      }

      verify(exactly = 0) { factory.reactiveConnection }
    }

  @Test
  fun `rejects blank channel`() =
    runTest {
      assertFailsWith<IllegalArgumentException> { publisher.publish(" ", "created", emptyMap()) }

      verify(exactly = 0) { factory.reactiveConnection }
    }

  @Test
  fun `closes connection when the script fails`() =
    runTest {
      every {
        scriptingCommands.eval<Any>(any(), ReturnType.VALUE, 1, *anyVararg())
      } returns Flux.error(IllegalStateException("boom"))

      assertFailsWith<IllegalStateException> { publisher.publish("orders", "created", emptyMap()) }

      verify(exactly = 1) { connection.closeLater() }
    }
}
