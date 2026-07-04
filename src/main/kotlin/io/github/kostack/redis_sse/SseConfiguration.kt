package io.github.kostack.redis_sse

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import tools.jackson.databind.ObjectMapper

@AutoConfiguration(after = [DataRedisReactiveAutoConfiguration::class, JacksonAutoConfiguration::class])
@ConditionalOnClass(ReactiveRedisConnectionFactory::class)
@ConditionalOnBean(ReactiveRedisConnectionFactory::class)
@EnableConfigurationProperties(SseProperties::class)
class SseConfiguration {
  @Bean
  @ConditionalOnBean(ObjectMapper::class)
  fun redisEventPublisher(
    factory: ReactiveRedisConnectionFactory,
    objectMapper: ObjectMapper,
    properties: SseProperties
  ): RedisEventPublisher = RedisEventPublisher(factory, objectMapper, properties)

  @Bean
  fun redisEventSubscriber(
    factory: ReactiveRedisConnectionFactory,
    properties: SseProperties
  ): RedisEventSubscriber = RedisEventSubscriber(factory, properties)
}
