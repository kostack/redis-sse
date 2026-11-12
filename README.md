# Redis SSE

Redis SSE is a Spring Boot auto-configuration library for publishing server-sent event payloads through Redis Streams and exposing them as Kotlin `Flow<ServerSentEvent<String>>`.

The package provides the following components when a reactive Redis connection factory is available:

- `RedisEventPublisher`
- `RedisEventSubscriber`
- `RedisPurger`

## Requirements

- Java 23
- Spring Boot 4.1
- Redis 5.0 or newer (Redis Streams)
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
  max-length: 10000
```

Defaults:

| Property | Default | Description |
| --- | ---: | --- |
| `redis-sse.stream-key-prefix` | `app:sse-events` | Prefix used for Redis stream keys. The channel is appended as `prefix:channel`. |
| `redis-sse.read-count` | `100` | Maximum number of stream records read in one Redis read call. |
| `redis-sse.block-timeout-in-seconds` | `10` | Redis stream blocking read timeout. |
| `redis-sse.retry-in-seconds` | `3` | SSE client retry duration sent with events. |
| `redis-sse.heartbeat-interval-in-seconds` | `15` | Interval for heartbeat SSE messages. |
| `redis-sse.max-length` | _none_ | Approximate maximum number of records kept per channel stream (`XADD MAXLEN ~`). When unset, streams are not trimmed. Must be positive. |

`max-length` trimming is approximate: Redis only removes whole internal nodes (100 entries each by
default), so a stream can hold somewhat more records than configured, and very small values may not
trim at all until the stream grows past a node. Trimming also applies to events that clients have not
read yet; see [Resuming after reconnects](#resuming-after-reconnects).

## Publishing Events

Inject `RedisEventPublisher` and publish a JSON-serializable map:

```kotlin
import io.github.kostack.redis_sse.RedisEventPublisher
import org.springframework.stereotype.Service
import kotlin.time.Duration.Companion.hours

@Service
class OrderEvents(
  private val publisher: RedisEventPublisher
) {
  suspend fun orderCreated(orderId: String) {
    publisher.publish(
      channel = "orders",
      type = "order-created",
      payload = mapOf("orderId" to orderId),
      ttl = 1.hours
    )
  }
}
```

The publisher writes one Redis Stream record containing:

- `channel`
- `type`
- `payload`

The `payload` field is a JSON string. `publish` returns the Redis record id of the new event.

The optional `ttl` argument (a `kotlin.time.Duration`, at least 1 millisecond) sets the expiration
of the entire channel stream. Each publish with a TTL refreshes that expiration. When `ttl` is
omitted or `null`, the publisher leaves any existing expiration unchanged, so a stream that was
given a TTL earlier keeps expiring on that schedule. Zero or negative TTLs are rejected.

The record append, the optional `max-length` trim and the optional expiration run atomically in a
single Lua script, so a stream is never left without its TTL after a partially failed publish.

The channel must not be blank.

## Subscribing To Events

Expose a WebFlux SSE endpoint by returning the subscriber flow:

```kotlin
import io.github.kostack.redis_sse.RedisEventSubscriber
import kotlinx.coroutines.flow.Flow
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestHeader
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
    @RequestParam(required = false) replayLimit: Int?,
    @RequestHeader("Last-Event-ID", required = false) lastEventId: String?
  ): Flow<ServerSentEvent<String>> =
    subscriber.subscribe(channel, replayLimit, lastEventId)
}
```

Clients can then connect to:

```text
GET /sse/orders
GET /sse/orders?replayLimit=0
GET /sse/orders?replayLimit=10
GET /sse/orders            (with header Last-Event-ID: 1700000000000-0)
```

`replayLimit` replays the last N messages from the Redis stream before continuing with live messages. Pass `0` to skip fetching previous messages and receive only new stream events. It must be greater than or equal to zero when provided. When `replayLimit` is omitted, the whole retained stream is replayed.

### Resuming after reconnects

`lastEventId` resumes streaming right after the given event id. Browsers send the id of the last received event in the `Last-Event-ID` header when an `EventSource` reconnects, so passing that header through lets clients continue without gaps or duplicates. When `lastEventId` is set, `replayLimit` is ignored.

`lastEventId` must be a Redis stream id such as `1700000000000-0`; any other value is rejected with `IllegalArgumentException`. Because the value comes from a client header, consider mapping that exception to `400 Bad Request` in your application.

If the events after `lastEventId` were already removed by `max-length` trimming, a TTL expiry or a purge, the client continues from the oldest event still retained, and the removed events are not delivered.

### Connections and errors

Each active subscription holds one dedicated Redis connection for blocking stream reads, which is closed when the client disconnects or the flow ends. The number of Redis connections therefore grows with the number of open SSE clients; keep Redis `maxclients` (10,000 by default) above your expected peak.

If reading from Redis fails, the subscriber logs the error and retries with exponential backoff (100 ms doubling up to 30 s) while heartbeats keep the connection open. Stream records without a `type` or `payload` field are logged and skipped.

## Purging Events

Inject `RedisPurger` to delete all events stored for a channel:

```kotlin
import io.github.kostack.redis_sse.RedisPurger
import org.springframework.stereotype.Service

@Service
class OrderEventCleanup(
  private val purger: RedisPurger
) {
  suspend fun purgeOrders() {
    purger.purge(channel = "orders")
  }
}
```

The purger deletes the Redis stream at `prefix:channel`. With the default configuration, purging the `orders` channel deletes `app:sse-events:orders`. This permanently removes all retained events for that channel; active subscribers can continue waiting for newly published events.

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

## Upgrading

Upgrading from versions before atomic publishing:

- `RedisEventPublisher.publish` takes `ttl` as a `kotlin.time.Duration` instead of a number of
  seconds. Replace `ttl = 3600` with `ttl = 1.hours` (`import kotlin.time.Duration.Companion.hours`).
- `publish` now returns the Redis record id as a `String`. Existing callers that ignore the result
  keep compiling.
- `payload` accepts `Map<String, Any?>`, so null values are allowed.
- `RedisEventSubscriber.subscribe` has a new optional `lastEventId` parameter. Existing calls keep
  working.

## License

Redis SSE is released under the MIT License. See `LICENSE`.
