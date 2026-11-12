package io.github.kostack.redis_sse

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.withContext
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.Limit
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.ReactiveStreamCommands
import org.springframework.data.redis.connection.stream.ByteBufferRecord
import org.springframework.data.redis.connection.stream.ReadOffset
import org.springframework.data.redis.connection.stream.StreamOffset
import org.springframework.data.redis.connection.stream.StreamReadOptions
import org.springframework.http.codec.ServerSentEvent
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.time.Duration
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class RedisEventSubscriber(
  private val factory: ReactiveRedisConnectionFactory,
  private val sseProperties: SseProperties
) {
  private data class StreamRecord(
    val id: String,
    val type: String,
    val payload: String
  )

  companion object {
    private val log: Logger = LoggerFactory.getLogger(RedisEventSubscriber::class.java)

    private const val STREAM_START_ID = "0-0"
    private val EVENT_ID_PATTERN = Regex("""\d+-\d+""")
    private val INITIAL_ERROR_BACKOFF = 100.milliseconds
    private val MAX_ERROR_BACKOFF = 30.seconds

    private val TYPE_FIELD: ByteBuffer = RedisStreamUtils.toBytes(SseKeys.RECORD_FIELD_TYPE).asReadOnlyBuffer()
    private val PAYLOAD_FIELD: ByteBuffer = RedisStreamUtils.toBytes(SseKeys.RECORD_FIELD_PAYLOAD).asReadOnlyBuffer()
  }

  fun subscribe(
    channel: String,
    replayLimit: Int? = null,
    lastEventId: String? = null
  ): Flow<ServerSentEvent<String>> =
    channelFlow {
      val heartbeatJob = launch { getHeartbeatsOutput().collect { send(it) } }
      getStreamOutput(channel, replayLimit, lastEventId).collect { send(it) }
      heartbeatJob.cancel()
    }

  private fun getStreamOutput(
    channel: String,
    replayLimit: Int?,
    lastEventId: String?
  ): Flow<ServerSentEvent<String>> =
    flow {
      replayLimit?.let { require(it >= 0) { "replayLimit must be greater than or equal to zero" } }
      lastEventId?.let {
        require(EVENT_ID_PATTERN.matches(it)) { "lastEventId must be a Redis stream id, was '$it'" }
      }

      val streamKey = RedisStreamUtils.toBytes(sseProperties.streamKey(channel))
      val connection = factory.reactiveConnection

      try {
        val streams = connection.streamCommands()

        var lastId =
          when {
            lastEventId != null -> lastEventId
            replayLimit == 0 -> fetchLastMessages(streams, streamKey, 1).lastOrNull()?.id?.value ?: STREAM_START_ID
            else -> STREAM_START_ID
          }

        if (lastEventId == null && replayLimit != null && replayLimit > 0) {
          for (record in fetchLastMessages(streams, streamKey, replayLimit)) {
            lastId = record.id.value
            if (emitRecord(record)) return@flow
          }
        }

        var backoff = INITIAL_ERROR_BACKOFF
        while (true) {
          val batch =
            try {
              readBatch(streams, streamKey, lastId)
            } catch (e: CancellationException) {
              throw e
            } catch (e: Exception) {
              log.error("Error while reading Redis stream {}, retrying in {}", channel, backoff, e)
              delay(backoff)
              backoff = (backoff * 2).coerceAtMost(MAX_ERROR_BACKOFF)
              continue
            }

          backoff = INITIAL_ERROR_BACKOFF
          for (record in batch) {
            lastId = record.id.value
            if (emitRecord(record)) return@flow
          }
        }
      } finally {
        withContext(NonCancellable) { connection.closeLater().awaitSingleOrNull() }
      }
    }

  private suspend fun FlowCollector<ServerSentEvent<String>>.emitRecord(record: ByteBufferRecord): Boolean {
    val sr =
      record.toStreamRecordOrNull() ?: run {
        log.warn("Skipping malformed Redis stream record {}", record.id.value)
        return false
      }

    emit(toSse(sr.id, sr.type, sr.payload))

    return sr.type == SseKeys.FINISHED_EVENT
  }

  private suspend fun fetchLastMessages(
    streams: ReactiveStreamCommands,
    streamKey: ByteBuffer,
    limit: Int
  ): List<ByteBufferRecord> =
    streams
      .xRevRange(streamKey, Range.unbounded(), Limit.limit().count(limit))
      .collectList()
      .awaitSingle()
      .reversed()

  private suspend fun readBatch(
    streams: ReactiveStreamCommands,
    streamKey: ByteBuffer,
    lastId: String
  ): List<ByteBufferRecord> {
    val options =
      StreamReadOptions
        .empty()
        .block(Duration.ofSeconds(sseProperties.blockTimeoutInSeconds))
        .count(sseProperties.readCount)

    return streams
      .read(
        Flux.just(
          ReactiveStreamCommands.ReadCommand
            .from(StreamOffset.create(streamKey, ReadOffset.from(lastId)))
            .withOptions(options)
        )
      ).flatMap { response ->
        response.output ?: Flux.empty()
      }.collectList()
      .awaitSingle()
  }

  private fun toSse(
    id: String,
    type: String,
    payload: String
  ): ServerSentEvent<String> =
    ServerSentEvent
      .builder<String>()
      .id(id)
      .event(type)
      .data(payload)
      .retry(Duration.ofSeconds(sseProperties.retryInSeconds))
      .build()

  private fun getHeartbeatsOutput(): Flow<ServerSentEvent<String>> =
    flow {
      while (true) {
        delay(sseProperties.heartbeatIntervalInSeconds.seconds)

        emit(
          ServerSentEvent
            .builder<String>()
            .event(SseKeys.HEARTBEAT_EVENT)
            .data("")
            .build()
        )
      }
    }

  private fun ByteBufferRecord.toStreamRecordOrNull(): StreamRecord? {
    val type = value[TYPE_FIELD] ?: return null
    val payload = value[PAYLOAD_FIELD] ?: return null

    return StreamRecord(
      id = id.value,
      type = RedisStreamUtils.fromBytes(type),
      payload = RedisStreamUtils.fromBytes(payload)
    )
  }
}
