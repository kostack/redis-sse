package io.github.kostack.redis_sse

import io.mockk.mockk
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.FilteredClassLoader
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import tools.jackson.databind.ObjectMapper

class SseConfigurationTest {
  private val contextRunner =
    ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(SseConfiguration::class.java))

  private val redisContextRunner =
    contextRunner.withBean(
      ReactiveRedisConnectionFactory::class.java,
      { mockk(relaxed = true) }
    )

  @Test
  fun `does not create redis sse beans without connection factory`() {
    contextRunner.run { context ->
      assertThat(context).doesNotHaveBean(RedisEventPublisher::class.java)
      assertThat(context).doesNotHaveBean(RedisEventSubscriber::class.java)
      assertThat(context).doesNotHaveBean(RedisPurger::class.java)
      assertThat(context).doesNotHaveBean(SseProperties::class.java)
    }
  }

  @Test
  fun `creates redis sse beans when connection factory is present`() {
    redisContextRunner
      .withBean(ObjectMapper::class.java, { mockk(relaxed = true) })
      .run { context ->
        assertThat(context).hasSingleBean(SseProperties::class.java)
        assertThat(context).hasSingleBean(RedisEventPublisher::class.java)
        assertThat(context).hasSingleBean(RedisEventSubscriber::class.java)
        assertThat(context).hasSingleBean(RedisPurger::class.java)
      }
  }

  @Test
  fun `skips only the publisher without an object mapper bean`() {
    redisContextRunner.run { context ->
      assertThat(context).doesNotHaveBean(RedisEventPublisher::class.java)
      assertThat(context).hasSingleBean(RedisEventSubscriber::class.java)
      assertThat(context).hasSingleBean(RedisPurger::class.java)
    }
  }

  @Test
  fun `skips only the publisher when jackson is not on the classpath`() {
    redisContextRunner
      .withClassLoader(FilteredClassLoader(ObjectMapper::class.java))
      .run { context ->
        assertThat(context).hasNotFailed()
        assertThat(context).doesNotHaveBean(RedisEventPublisher::class.java)
        assertThat(context).hasSingleBean(RedisEventSubscriber::class.java)
        assertThat(context).hasSingleBean(RedisPurger::class.java)
      }
  }

  @Test
  fun `backs off when the application defines its own beans`() {
    val publisher = mockk<RedisEventPublisher>()
    val subscriber = mockk<RedisEventSubscriber>()
    val purger = mockk<RedisPurger>()

    redisContextRunner
      .withBean(ObjectMapper::class.java, { mockk(relaxed = true) })
      .withBean(RedisEventPublisher::class.java, { publisher })
      .withBean(RedisEventSubscriber::class.java, { subscriber })
      .withBean(RedisPurger::class.java, { purger })
      .run { context ->
        assertThat(context.getBean(RedisEventPublisher::class.java)).isSameAs(publisher)
        assertThat(context.getBean(RedisEventSubscriber::class.java)).isSameAs(subscriber)
        assertThat(context.getBean(RedisPurger::class.java)).isSameAs(purger)
      }
  }
}
