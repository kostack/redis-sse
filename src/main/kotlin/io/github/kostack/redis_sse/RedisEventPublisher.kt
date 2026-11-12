package io.github.kostack.redis_sse

import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.withContext
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.ReturnType
import tools.jackson.databind.ObjectMapper
import java.nio.ByteBuffer
import kotlin.time.Duration

class RedisEventPublisher(
  private val factory: ReactiveRedisConnectionFactory,
  private val objectMapper: ObjectMapper,
  private val properties: SseProperties
) {
  suspend fun publish(
    channel: String,
    type: String,
    payload: Map<String, Any?> = emptyMap(),
    ttl: Duration? = null
  ): String {
    require(ttl == null || ttl.inWholeMilliseconds > 0) { "ttl must be at least 1 millisecond, was $ttl" }

    val streamKey = properties.streamKey(channel)
    val keysAndArgs =
      listOf(
        streamKey,
        ttl?.inWholeMilliseconds?.toString().orEmpty(),
        properties.maxLength?.toString().orEmpty(),
        SseKeys.RECORD_FIELD_CHANNEL,
        channel,
        SseKeys.RECORD_FIELD_TYPE,
        type,
        SseKeys.RECORD_FIELD_PAYLOAD,
        objectMapper.writeValueAsString(payload)
      ).map(RedisStreamUtils::toBytes)

    val connection = factory.reactiveConnection
    try {
      val result =
        connection
          .scriptingCommands()
          .eval<Any>(
            PUBLISH_SCRIPT.duplicate(),
            ReturnType.VALUE,
            1,
            *keysAndArgs.toTypedArray()
          ).next()
          .awaitSingle()

      return when (result) {
        is ByteBuffer -> RedisStreamUtils.fromBytes(result)
        is ByteArray -> String(result, Charsets.UTF_8)
        else -> result.toString()
      }
    } finally {
      withContext(NonCancellable) { connection.closeLater().awaitSingleOrNull() }
    }
  }

  private companion object {
    private val PUBLISH_SCRIPT: ByteBuffer =
      RedisStreamUtils
        .toBytes(
          """
          local args = { KEYS[1] }
          if ARGV[2] ~= '' then
            table.insert(args, 'MAXLEN')
            table.insert(args, '~')
            table.insert(args, ARGV[2])
          end
          table.insert(args, '*')
          for i = 3, #ARGV do
            table.insert(args, ARGV[i])
          end
          local id = redis.call('XADD', unpack(args))
          if ARGV[1] ~= '' then
            redis.call('PEXPIRE', KEYS[1], ARGV[1])
          end
          return id
          """.trimIndent()
        ).asReadOnlyBuffer()
  }
}
