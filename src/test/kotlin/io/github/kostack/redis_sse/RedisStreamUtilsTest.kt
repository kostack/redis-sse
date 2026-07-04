package io.github.kostack.redis_sse

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class RedisStreamUtilsTest {
  @Test
  fun `converts string to utf8 byte buffer`() {
    val buffer = RedisStreamUtils.toBytes("hello")

    assertThat(buffer.remaining()).isEqualTo(5)
    assertThat(StandardCharsets.UTF_8.decode(buffer).toString()).isEqualTo("hello")
  }

  @Test
  fun `converts array backed byte buffer using current position and remaining bytes`() {
    val buffer = ByteBuffer.wrap("prefix-value-suffix".toByteArray(StandardCharsets.UTF_8))
    buffer.position("prefix-".length)
    buffer.limit("prefix-value".length)

    val value = RedisStreamUtils.fromBytes(buffer)

    assertThat(value).isEqualTo("value")
    assertThat(buffer.position()).isEqualTo("prefix-".length)
    assertThat(buffer.limit()).isEqualTo("prefix-value".length)
  }

  @Test
  fun `converts direct byte buffer without backing array`() {
    val bytes = "direct-value".toByteArray(StandardCharsets.UTF_8)
    val buffer = ByteBuffer.allocateDirect(bytes.size)
    buffer.put(bytes)
    buffer.flip()

    val value = RedisStreamUtils.fromBytes(buffer)

    assertThat(value).isEqualTo("direct-value")
    assertThat(buffer.position()).isEqualTo(0)
  }

  @Test
  fun `round trips utf8 text`() {
    val value = "hello Привет Γεια"

    val result = RedisStreamUtils.fromBytes(RedisStreamUtils.toBytes(value))

    assertThat(result).isEqualTo(value)
  }
}
