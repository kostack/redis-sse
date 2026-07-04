package io.github.kostack.redis_sse

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.transformWhile
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactor.awaitSingle
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
  }

  fun subscribe(
    channel: String,
    replayLimit: Int? = null
  ): Flow<ServerSentEvent<String>> =
    channelFlow {
      val heartbeatJob = launch { getHeartbeatsOutput().collect { send(it) } }
      getStreamOutput(channel, replayLimit).collect { send(it) }
      heartbeatJob.cancel()
    }

  private fun getStreamOutput(
    channel: String,
    replayLimit: Int?
  ): Flow<ServerSentEvent<String>> =
    channelFlow {
      var lastId = ReadOffset.from("0-0")

      val streams = factory.reactiveConnection.streamCommands()
      val streamKey = RedisStreamUtils.toBytes("${sseProperties.streamKeyPrefix}:$channel")

      if (replayLimit != null) {
        for (record in fetchLastMessages(channel, replayLimit)) {
          val sr = record.toStreamRecord()
          lastId = ReadOffset.from(sr.id)
          send(toSse(sr.id, sr.type, sr.payload))
          if (sr.type == SseKeys.FINISHED_EVENT) return@channelFlow
        }
      }

      var finished = false
      while (!finished) {
        readBatch(streams, streamKey, lastId)
          .catch { e ->
            log.error("Error while reading Redis stream", e)
          }.map { it.toStreamRecord() }
          .transformWhile { sr ->
            emit(sr)
            sr.type != SseKeys.FINISHED_EVENT
          }.collect { sr ->
            lastId = ReadOffset.from(sr.id)
            send(toSse(sr.id, sr.type, sr.payload))
            if (sr.type == SseKeys.FINISHED_EVENT) finished = true
          }
      }
    }

  private suspend fun fetchLastMessages(
    channel: String,
    limit: Int
  ): List<ByteBufferRecord> {
    val streamKey = RedisStreamUtils.toBytes("${sseProperties.streamKeyPrefix}:$channel")
    val count = validateReplayLimit(limit)

    return factory.reactiveConnection
      .streamCommands()
      .xRevRange(streamKey, Range.unbounded(), Limit.limit().count(count))
      .collectList()
      .awaitSingle()
      .reversed()
  }

  private fun validateReplayLimit(limit: Int): Int {
    require(limit > 0) { "replayLimit must be greater than zero" }
    require(limit <= Int.MAX_VALUE) { "replayLimit must be less than or equal to ${Int.MAX_VALUE}" }

    return limit
  }

  private fun readBatch(
    streams: ReactiveStreamCommands,
    streamKey: ByteBuffer,
    lastId: ReadOffset
  ): Flow<ByteBufferRecord> {
    val options =
      StreamReadOptions
        .empty()
        .block(Duration.ofSeconds(sseProperties.blockTimeoutInSeconds))
        .count(sseProperties.readCount)

    return streams
      .read(
        Flux.just(
          ReactiveStreamCommands.ReadCommand
            .from(StreamOffset.create(streamKey, lastId))
            .withOptions(options)
        )
      ).flatMap { response ->
        response.output ?: Flux.empty()
      }.asFlow()
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

  private fun ByteBufferRecord.toStreamRecord() =
    StreamRecord(
      id = id.value,
      type = getRecordValue(this, SseKeys.RECORD_FIELD_TYPE),
      payload = getRecordValue(this, SseKeys.RECORD_FIELD_PAYLOAD)
    )

  private fun getRecordValue(
    record: ByteBufferRecord,
    key: String
  ): String {
    val value =
      record.value[RedisStreamUtils.toBytes(key)]
        ?: error("Redis stream record is missing key: $key")

    return RedisStreamUtils.fromBytes(value)
  }
}
