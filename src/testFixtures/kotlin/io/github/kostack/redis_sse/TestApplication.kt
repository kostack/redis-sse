package io.github.kostack.redis_sse

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class TestApplication

fun main(args: Array<String>) {
  runApplication<TestApplication>(*args)
}
