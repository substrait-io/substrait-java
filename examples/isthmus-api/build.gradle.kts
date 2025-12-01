plugins {
  // Apply the application plugin to add support for building a CLI application in Java.
  id("application")
  alias(libs.plugins.spotless)
  id("substrait.java-conventions")
}

repositories {
  // Use Maven Central for resolving dependencies.
  mavenCentral()
}

dependencies {
  implementation(project(":isthmus"))
  implementation(libs.calcite.core)
  implementation(libs.calcite.server)
  // For a real Spark application, these would not be required since they would be in the Spark
  // server classpath
  runtimeOnly(libs.spark.core)
  runtimeOnly(libs.spark.hive)
}

application { mainClass = "io.substrait.examples.IsthmusAppExamples" }

tasks.named<Test>("test") {
  // Use JUnit Platform for unit tests.
  useJUnitPlatform()
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

tasks.pmdMain { dependsOn(":core:shadowJar") }
