import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach
import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
  `maven-publish`
  id("java")
  id("idea")
  alias(libs.plugins.gradle.extensions)
  alias(libs.plugins.spotless)
  alias(libs.plugins.nmcp).apply(false)
  alias(libs.plugins.nmcp.aggregation)
}

repositories { mavenCentral() }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

nmcpAggregation {
  centralPortal {
    username =
      System.getenv("MAVENCENTRAL_USERNAME").takeUnless { it.isNullOrEmpty() }
        ?: extra["MAVENCENTRAL_USERNAME"].toString()
    password =
      System.getenv("MAVENCENTRAL_PASSWORD").takeUnless { it.isNullOrEmpty() }
        ?: extra["MAVENCENTRAL_PASSWORD"].toString()
    publishingType = "AUTOMATIC"
  }
}

dependencies {
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter.api)
  testImplementation(libs.junit.jupiter.engine)
  implementation(libs.slf4j.api)
  annotationProcessor(libs.immutables.processor)
  compileOnly(libs.immutables.annotations)

  nmcpAggregation(project(":core"))
  nmcpAggregation(project(":spark"))
  nmcpAggregation(project(":isthmus"))
}

allprojects {
  repositories { mavenCentral() }

  tasks.configureEach<Test> {
    useJUnitPlatform()
    testLogging { exceptionFormat = TestExceptionFormat.FULL }
  }

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
        forbidWildcardImports()
      }
    }
  }
}

// run spotlessApply for build-logic module when running spotlessApply on the root
tasks.spotlessApply { dependsOn(gradle.includedBuilds.map { it.task(":spotlessApply") }) }

// run check for build-logic module when running check on the root
tasks.check { dependsOn(gradle.includedBuilds.map { it.task(":check") }) }
