pluginManagement {
  val kotlinVersion: String by settings
  plugins {
    kotlin("jvm") version kotlinVersion
    kotlin("kapt") version kotlinVersion
    kotlin("plugin.spring") version kotlinVersion
  }
}

rootProject.name = "redis-sse"
