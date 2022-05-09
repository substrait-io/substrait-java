import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach

plugins {
  id("java")
  id("idea")
  id("com.github.vlsi.gradle-extensions") version "1.74"
  id("com.diffplug.spotless") version "6.4.2"
}

repositories { mavenCentral() }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencies {
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
  implementation("org.slf4j:slf4j-jdk14:1.7.30")
  annotationProcessor("org.immutables:value:2.8.8")
  compileOnly("org.immutables:value-annotations:2.8.8")
}

val submodulesUpdate by
  tasks.creating(Exec::class) {
    group = "Build Setup"
    description = "Updates (and inits) substrait git submodule"
    commandLine = listOf("git", "submodule", "update", "--init", "--recursive")
  }

allprojects {
  repositories { mavenCentral() }

  tasks.configureEach<Test> { useJUnitPlatform() }
  tasks.withType<JavaCompile> { dependsOn(submodulesUpdate) }

  tasks.withType<JavaCompile>().configureEach { options.compilerArgs.add("--enable-preview") }

  tasks.withType<Test>().configureEach { jvmArgs("--enable-preview") }

  plugins.withType<SpotlessPlugin>().configureEach {
    configure<SpotlessExtension> { kotlinGradle { ktfmt().googleStyle() } }
  }

  group = "io.substrait"
  version = "1.0-SNAPSHOT"
}
