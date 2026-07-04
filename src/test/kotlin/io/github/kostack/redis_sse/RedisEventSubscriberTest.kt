package io.github.kostack.redis_sse

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.reactivestreams.Publisher
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.Limit
import org.springframework.data.redis.connection.ReactiveRedisConnection
import org.springframework.data.redis.connection.ReactiveRedisConnection.CommandResponse
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.ReactiveStreamCommands
import org.springframework.data.redis.connection.ReactiveStreamCommands.ReadCommand
import org.springframework.data.redis.connection.stream.ByteBufferRecord
import org.springframework.data.redis.connection.stream.RecordId
import org.springframework.data.redis.connection.stream.StreamRecords
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.time.Duration
import kotlin.test.assertFailsWith

class RedisEventSubscriberTest {
  private val factory = mockk<ReactiveRedisConnectionFactory>()
  private val connection = mockk<ReactiveRedisConnection>()
  private val streamCommands = mockk<ReactiveStreamCommands>()
  private val properties =
    SseProperties(
      streamKeyPrefix = "test:sse-events",
      readCount = 25,
      blockTimeoutInSeconds = 2,
      retryInSeconds = 7,
      heartbeatIntervalInSeconds = 60
    )

  private val subscriber = RedisEventSubscriber(factory, properties)

  @Test
  fun `emits replayed events in chronological order and stops on finished event`() =
    runTest {
      val capturedKey = mutableListOf<ByteBuffer>()
      val capturedLimit = mutableListOf<Limit>()
      every { factory.reactiveConnection } returns connection
      every { connection.streamCommands() } returns streamCommands
      every {
        streamCommands.xRevRange(
          capture(capturedKey),
          any<Range<String>>(),
          capture(capturedLimit)
        )
      } returns
        Flux.just(
          record("2-0", SseKeys.FINISHED_EVENT, """{}"""),
          record("1-0", "message", """{"value":1}""")
        )

      val events = subscriber.subscribe("orders", replayLimit = 2).toList()

      assertThat(events).hasSize(2)
      assertThat(events.map { it.id() }).containsExactly("1-0", "2-0")
      assertThat(events.map { it.event() }).containsExactly("message", SseKeys.FINISHED_EVENT)
      assertThat(events.map { it.data() }).containsExactly("""{"value":1}""", """{}""")
      assertThat(capturedKey.single().asString()).isEqualTo("test:sse-events:orders")
      assertThat(capturedLimit.single().count).isEqualTo(2)

      verify(exactly = 0) { streamCommands.read(any<Publisher<ReadCommand>>()) }
    }

  @Test
  fun `emits live stream events using configured read options`() =
    runTest {
      val capturedReadCommands = mutableListOf<ReadCommand>()
      every { factory.reactiveConnection } returns connection
      every { connection.streamCommands() } returns streamCommands
      every { streamCommands.read(any<Publisher<ReadCommand>>()) } answers {
        val command = Flux.from(firstArg<Publisher<ReadCommand>>()).blockFirst()!!
        capturedReadCommands += command

        Flux.just(
          CommandResponse(
            command,
            Flux.just(record("3-0", SseKeys.FINISHED_EVENT, """{"done":true}"""))
          )
        )
      }

      val events = subscriber.subscribe("orders").toList()
      val command = capturedReadCommands.single()
      val streamOffset = command.streamOffsets.single()

      assertThat(events).hasSize(1)
      assertThat(events.single().id()).isEqualTo("3-0")
      assertThat(events.single().event()).isEqualTo(SseKeys.FINISHED_EVENT)
      assertThat(events.single().data()).isEqualTo("""{"done":true}""")
      assertThat(events.single().retry()).isEqualTo(Duration.ofSeconds(7))
      assertThat(streamOffset.key.asString()).isEqualTo("test:sse-events:orders")
      assertThat(streamOffset.offset.offset).isEqualTo("0-0")
      assertThat(command.readOptions!!.count).isEqualTo(25)
      assertThat(command.readOptions!!.isBlocking).isTrue()
    }

  @Test
  fun `rejects non positive replay limit`() =
    runTest {
      every { factory.reactiveConnection } returns connection
      every { connection.streamCommands() } returns streamCommands

      val exception =
        assertFailsWith<IllegalArgumentException> {
          subscriber.subscribe("orders", replayLimit = 0).toList()
        }

      assertThat(exception).hasMessage("replayLimit must be greater than zero")
    }

  private fun record(
    id: String,
    type: String,
    payload: String
  ): ByteBufferRecord =
    StreamRecords
      .rawBuffer(
        mapOf(
          bytes(SseKeys.RECORD_FIELD_TYPE) to bytes(type),
          bytes(SseKeys.RECORD_FIELD_PAYLOAD) to bytes(payload)
        )
      ).withId(RecordId.of(id))
      .withStreamKey(bytes("test:sse-events:orders"))

  private fun bytes(value: String): ByteBuffer = RedisStreamUtils.toBytes(value)

  private fun ByteBuffer.asString(): String = RedisStreamUtils.fromBytes(this)
}
