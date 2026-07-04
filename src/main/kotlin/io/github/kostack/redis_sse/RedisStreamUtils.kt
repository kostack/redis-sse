package io.github.kostack.redis_sse

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object RedisStreamUtils {
  private val charset = StandardCharsets.UTF_8

  fun fromBytes(byteBuffer: ByteBuffer): String {
    val duplicate = byteBuffer.duplicate()

    return if (duplicate.hasArray()) {
      String(
        duplicate.array(),
        duplicate.arrayOffset() + duplicate.position(),
        duplicate.remaining(),
        charset
      )
    } else {
      charset.decode(duplicate).toString()
    }
  }

  fun toBytes(value: String): ByteBuffer = ByteBuffer.wrap(value.toByteArray(charset))
}
