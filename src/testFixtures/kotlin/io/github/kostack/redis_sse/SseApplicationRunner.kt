package io.github.kostack.redis_sse

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.stereotype.Component
import kotlin.time.Duration.Companion.seconds

@Component
class SseApplicationRunner(
  private val publisher: RedisEventPublisher
) : ApplicationRunner {
  override fun run(args: ApplicationArguments): Unit =
    runBlocking {
      for (i in 1..10) {
        delay(5.seconds)
        publisher.publish("test", "test", mapOf("key" to "value-$i"))
      }
      delay(5.seconds)
      log.info("Publishing event: finished")
      // publisher.publish("test", "finished", emptyMap())
    }

  companion object {
    private val log = LoggerFactory.getLogger(this::class.java)
  }
}
