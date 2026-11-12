package io.github.kostack.redis_sse

import kotlinx.coroutines.flow.Flow
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.http.codec.ServerSentEvent
import tools.jackson.databind.ObjectMapper

@AutoConfiguration(after = [DataRedisReactiveAutoConfiguration::class, JacksonAutoConfiguration::class])
@ConditionalOnClass(ReactiveRedisConnectionFactory::class, ServerSentEvent::class, Flow::class)
@ConditionalOnBean(ReactiveRedisConnectionFactory::class)
@EnableConfigurationProperties(SseProperties::class)
class SseConfiguration {
  @Bean
  @ConditionalOnMissingBean
  fun redisEventSubscriber(
    factory: ReactiveRedisConnectionFactory,
    properties: SseProperties
  ): RedisEventSubscriber = RedisEventSubscriber(factory, properties)

  @Bean
  @ConditionalOnMissingBean
  fun redisPurger(
    factory: ReactiveRedisConnectionFactory,
    properties: SseProperties
  ): RedisPurger = RedisPurger(ReactiveStringRedisTemplate(factory), properties)

  @Configuration(proxyBeanMethods = false)
  @ConditionalOnClass(ObjectMapper::class)
  @ConditionalOnBean(ObjectMapper::class)
  class PublisherConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun redisEventPublisher(
      factory: ReactiveRedisConnectionFactory,
      objectMapper: ObjectMapper,
      properties: SseProperties
    ): RedisEventPublisher = RedisEventPublisher(factory, objectMapper, properties)
  }
}
