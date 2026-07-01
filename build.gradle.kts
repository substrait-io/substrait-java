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
  annotationProcessor(libs.immutables.value)
  compileOnly(libs.immutables.annotations)

  nmcpAggregation(project(":core"))
  nmcpAggregation(project(":spark:spark-3.4_2.12"))
  nmcpAggregation(project(":spark:spark-3.5_2.12"))
  nmcpAggregation(project(":spark:spark-4.0_2.13"))
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
      // latest ktfmt version is 0.61
      // https://github.com/facebook/ktfmt/releases
      kotlinGradle { ktfmt("0.61").googleStyle() }
      java {
        target("src/*/java/**/*.java")
        // google-java-format 1.28.0: earlier versions (e.g. 1.23.0, which ktfmt 0.61 bundles) call
        // com.sun.tools.javac internals that changed in recent JDKs, so Spotless throws
        // NoSuchMethodError/NoClassDefFoundError when the daemon runs on a newer JDK (e.g. 25).
        // 1.28.0 is the newest release that still runs on JDK 17 (1.29.0+ requires JDK 21+),
        // matching the daemon/CI toolchain. This Java step has its own formatter classpath,
        // independent of ktfmt's bundled copy (used only for Kotlin), so they need not match.
        googleJavaFormat("1.28.0")
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
