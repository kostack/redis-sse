package io.github.kostack.redis_sse

import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import tools.jackson.databind.ObjectMapper

class SseConfigurationTest {
  private val contextRunner =
    ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(SseConfiguration::class.java))

  @Test
  fun `does not create redis sse beans without connection factory`() {
    contextRunner.run { context ->
      assertThat(context).doesNotHaveBean(RedisEventPublisher::class.java)
      assertThat(context).doesNotHaveBean(RedisEventSubscriber::class.java)
      assertThat(context).doesNotHaveBean(SseProperties::class.java)
    }
  }

  @Test
  fun `creates redis sse beans when connection factory is present`() {
    contextRunner
      .withBean(
        ReactiveRedisConnectionFactory::class.java,
        { mockk(relaxed = true) }
      ).withBean(
        ObjectMapper::class.java,
        { mockk(relaxed = true) }
      ).run { context ->
        assertThat(context).hasSingleBean(SseProperties::class.java)
        assertThat(context).hasSingleBean(RedisEventPublisher::class.java)
        assertThat(context).hasSingleBean(RedisEventSubscriber::class.java)
      }
  }
}
