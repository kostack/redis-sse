import org.gradle.testing.jacoco.tasks.JacocoCoverageVerification
import org.gradle.testing.jacoco.tasks.JacocoReport

plugins {
  kotlin("jvm") version "2.3.0"
  kotlin("plugin.spring") version "2.3.0"
  id("org.springframework.boot") version "4.1.0"
  id("io.spring.dependency-management") version "1.1.7"
  id("com.vanniktech.maven.publish") version "0.33.0"
  id("java-test-fixtures")
  id("com.diffplug.spotless") version "8.4.0"
  jacoco
}

group = "io.github.kostack"
version = providers.gradleProperty("projectVersion").getOrElse("1.0.0")
description = "Redis SSE"

java {
  toolchain {
    languageVersion = JavaLanguageVersion.of(23)
  }
}

repositories {
  mavenCentral()
}

dependencyManagement {
  imports {
    mavenBom(org.springframework.boot.gradle.plugin.SpringBootPlugin.BOM_COORDINATES)
  }
}

dependencies {
  // API
  api("org.springframework.boot:spring-boot-starter-webflux")
  api("org.springframework.boot:spring-boot-starter-data-redis-reactive")

  // Kotlin
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
  implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
  implementation("org.jetbrains.kotlin:kotlin-reflect")

  testImplementation("org.springframework.boot:spring-boot-starter-test")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  testImplementation("com.ninja-squad:springmockk:5.0.1")
  testImplementation("io.mockk:mockk:1.14.11")
  testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
  testFixturesImplementation(kotlin("stdlib"))
  testImplementation(kotlin("test"))
  testImplementation("io.projectreactor:reactor-test")

  testFixturesImplementation("org.springframework.boot:spring-boot-starter-webflux")
  testFixturesImplementation("org.springframework.boot:spring-boot-autoconfigure")
  testFixturesImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
  testFixturesImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")
  testFixturesImplementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
}

kotlin {
  compilerOptions {
    freeCompilerArgs.addAll("-Xjsr305=strict", "-Xannotation-default-target=param-property")
  }
}

tasks.withType<Test> {
  useJUnitPlatform()
}

val installGitHook =
  tasks.register("installGitHook") {
    group = "build setup"
    description = "Installs a git pre-commit hook to enforce formatting rules"
    doLast {
      val hookSource = file("scripts/pre-commit")
      val hookTarget = file(".git/hooks/pre-commit")
      if (!hookTarget.exists()) {
        hookSource.copyTo(hookTarget, overwrite = false)
      }
      hookTarget.setExecutable(true)
    }
  }

tasks.named("assemble") {
  dependsOn(installGitHook)
}

tasks.named<Test>("test") {
  useJUnitPlatform()
  dependsOn(installGitHook)

  finalizedBy(tasks.named("jacocoTestReport"))
  reports.junitXml.required.set(true)

  testLogging {
    events("passed", "skipped", "failed")
    showStandardStreams = true
  }
  jvmArgs("-XX:+EnableDynamicAgentLoading")
}

tasks.named<JacocoReport>("jacocoTestReport") {
  dependsOn(tasks.named("test"))

  reports {
    xml.required.set(true)
    html.required.set(true)
    csv.required.set(false)
  }

  reports.html.outputLocation.set(layout.buildDirectory.dir("jacocoHtml"))
  reports.xml.outputLocation.set(layout.buildDirectory.file("jacocoReport.xml"))
}

tasks.named<JacocoCoverageVerification>("jacocoTestCoverageVerification") {
  violationRules {
    rule {
      limit {
        minimum = "0.6".toBigDecimal()
      }
    }
  }
}

tasks.named("check") {
  dependsOn(tasks.named("jacocoTestCoverageVerification"))
  dependsOn(tasks.named("spotlessCheck"))
  dependsOn(tasks.named("test"))
}

tasks.named("bootJar") {
  enabled = false
}

tasks.named("jar") {
  enabled = true
}

spotless {
  kotlin {
    target("src/**/*.kt")
    ktlint()
  }
  kotlinGradle {
    target("*.kts")
    ktlint()
  }
}

mavenPublishing {
  publishToMavenCentral()
  signAllPublications()

  coordinates(
    groupId = group.toString(),
    artifactId = "redis-sse",
    version = version.toString()
  )

  pom {
    name.set("Redis SSE")
    description.set(project.description)
    url.set("https://github.com/kostack/redis-sse")

    licenses {
      license {
        name.set("MIT License")
        url.set("https://opensource.org/licenses/MIT")
      }
    }

    developers {
      developer {
        id.set("zender")
        name.set("Nikolay Georgiev")
        url.set("https://github.com/zender")
      }
    }

    scm {
      connection.set("scm:git:git://github.com/kostack/redis-sse.git")
      developerConnection.set("scm:git:ssh://github.com/kostack/redis-sse.git")
      url.set("https://github.com/kostack/redis-sse")
    }
  }
}
