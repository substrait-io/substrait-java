import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.github.vlsi.gradle-extensions") version "1.74"
  id("com.diffplug.spotless") version "6.19.0"
  id("org.jreleaser") version "1.18.0" apply false
}

var IMMUTABLES_VERSION = properties.get("immutables.version")
var JUNIT_VERSION = properties.get("junit.version")
var SLF4J_VERSION = properties.get("slf4j.version")

repositories { mavenCentral() }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencies {
  testImplementation("org.junit.jupiter:junit-jupiter-api:${JUNIT_VERSION}")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${JUNIT_VERSION}")
  implementation("org.slf4j:slf4j-api:${SLF4J_VERSION}")
  annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")
  compileOnly("org.immutables:value-annotations:${IMMUTABLES_VERSION}")
}

val submodulesUpdate by
  tasks.registering(Exec::class) {
    group = "Build Setup"
    description = "Updates (and inits) substrait git submodule"
    commandLine = listOf("git", "submodule", "update", "--init", "--recursive")
  }

allprojects {
  repositories { mavenCentral() }

  tasks.configureEach<Test> {
    useJUnitPlatform()
    testLogging { exceptionFormat = TestExceptionFormat.FULL }
  }
  tasks.withType<JavaCompile> { dependsOn(submodulesUpdate) }

  group = "io.substrait"
  version = "${version}"

  plugins.withType<SpotlessPlugin>().configureEach {
    configure<SpotlessExtension> {
      kotlinGradle { ktfmt().googleStyle() }
      java {
        target("src/*/java/**/*.java")
        googleJavaFormat()
        removeUnusedImports()
        trimTrailingWhitespace()
      }
    }
  }
}
