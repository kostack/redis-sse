package io.github.kostack.redis_sse

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
  private val redisEventSubscriber: RedisEventSubscriber
) {
  @GetMapping(
    value = ["/{channel}"],
    produces = [MediaType.TEXT_EVENT_STREAM_VALUE]
  )
  fun stream(
    @PathVariable channel: String,
    @RequestParam(required = false) replayLimit: Int?
  ): Flow<ServerSentEvent<String>> = redisEventSubscriber.subscribe(channel, replayLimit)
}
