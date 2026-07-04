# Redis SSE

Redis SSE is a Spring Boot auto-configuration library for publishing server-sent event payloads through Redis Streams and exposing them as Kotlin `Flow<ServerSentEvent<String>>`.

The package provides two beans when a reactive Redis connection factory is available:

- `RedisEventPublisher`
- `RedisEventSubscriber`

## Requirements

- Java 23
- Spring Boot 4.1
- Reactive Redis configured through Spring Boot
- WebFlux for SSE endpoints

## Installation

Gradle Kotlin DSL:

```kotlin
dependencies {
  implementation("io.github.kostack:redis-sse:<version>")
}
```

Gradle Groovy DSL:

```groovy
dependencies {
  implementation 'io.github.kostack:redis-sse:<version>'
}
```

Maven:

```xml
<dependency>
  <groupId>io.github.kostack</groupId>
  <artifactId>redis-sse</artifactId>
  <version>VERSION</version>
</dependency>
```

## Configuration

The package is registered through Spring Boot auto-configuration. No `@Enable...` annotation is required in the consuming application.

Configure Redis as usual:

```yaml
spring:
  data:
    redis:
      host: localhost
      port: 6379
```

Optional Redis SSE settings:

```yaml
redis-sse:
  stream-key-prefix: app:sse-events
  read-count: 100
  block-timeout-in-seconds: 10
  retry-in-seconds: 3
  heartbeat-interval-in-seconds: 15
```

Defaults:

| Property | Default | Description |
| --- | ---: | --- |
| `redis-sse.stream-key-prefix` | `app:sse-events` | Prefix used for Redis stream keys. The channel is appended as `prefix:channel`. |
| `redis-sse.read-count` | `100` | Maximum number of stream records read in one Redis read call. |
| `redis-sse.block-timeout-in-seconds` | `10` | Redis stream blocking read timeout. |
| `redis-sse.retry-in-seconds` | `3` | SSE client retry duration sent with events. |
| `redis-sse.heartbeat-interval-in-seconds` | `15` | Interval for heartbeat SSE messages. |

## Publishing Events

Inject `RedisEventPublisher` and publish a JSON-serializable map:

```kotlin
import io.github.kostack.redis_sse.RedisEventPublisher
import org.springframework.stereotype.Service

@Service
class OrderEvents(
  private val publisher: RedisEventPublisher
) {
  suspend fun orderCreated(orderId: String) {
    publisher.publish(
      channel = "orders",
      type = "order-created",
      payload = mapOf("orderId" to orderId)
    )
  }
}
```

The publisher writes one Redis Stream record containing:

- `channel`
- `type`
- `payload`

The `payload` field is a JSON string.

## Subscribing To Events

Expose a WebFlux SSE endpoint by returning the subscriber flow:

```kotlin
import io.github.kostack.redis_sse.RedisEventSubscriber
import kotlinx.coroutines.flow.Flow
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/sse")
class SseController(
  private val subscriber: RedisEventSubscriber
) {
  @GetMapping(
    value = ["/{channel}"],
    produces = [MediaType.TEXT_EVENT_STREAM_VALUE]
  )
  fun stream(
    @PathVariable channel: String,
    @RequestParam(required = false) replayLimit: Int?
  ): Flow<ServerSentEvent<String>> =
    subscriber.subscribe(channel, replayLimit)
}
```

Clients can then connect to:

```text
GET /sse/orders
GET /sse/orders?replayLimit=10
```

`replayLimit` replays the last N messages from the Redis stream before continuing with live messages. It must be greater than zero when provided.

## Event Completion

Publishing an event with type `finished` ends the subscriber flow for that channel:

```kotlin
publisher.publish(
  channel = "orders",
  type = "finished",
  payload = emptyMap()
)
```

Heartbeat events are emitted with type `heartbeat` and empty data while the stream remains open.

## License

Redis SSE is released under the MIT License. See `LICENSE`.
