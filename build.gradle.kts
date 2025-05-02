import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.github.vlsi.gradle-extensions") version "1.74"
  id("com.diffplug.spotless") version "6.19.0"
  id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
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
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
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
    val javaToolchains = project.extensions.getByType<JavaToolchainService>()
    useJUnitPlatform()
    javaLauncher.set(javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(11)) })
  }
  tasks.withType<JavaCompile> {
    sourceCompatibility = "17"
    if (project.name != "core") {
      options.release.set(11)
    } else {
      options.release.set(8)
    }
    dependsOn(submodulesUpdate)
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
      }
    }
  }
}

nexusPublishing {
  repositories {
    create("sonatype") {
      val sonatypeUser =
        System.getenv("SONATYPE_USER").takeUnless { it.isNullOrEmpty() }
          ?: extra["SONATYPE_USER"].toString()
      val sonatypePassword =
        System.getenv("SONATYPE_PASSWORD").takeUnless { it.isNullOrEmpty() }
          ?: extra["SONATYPE_PASSWORD"].toString()
      nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))

      snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
      username.set(sonatypeUser)
      password.set(sonatypePassword)
    }
  }
}
