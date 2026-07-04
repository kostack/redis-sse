package io.github.kostack.redis_sse

object SseKeys {
  const val RECORD_FIELD_CHANNEL = "channel"
  const val RECORD_FIELD_TYPE = "type"
  const val RECORD_FIELD_PAYLOAD = "payload"
  const val HEARTBEAT_EVENT = "heartbeat"
  const val FINISHED_EVENT = "finished"
}
