plugins {
  // Apply the application plugin to add support for building a CLI application in Java.
  id("java")
  id("com.diffplug.spotless") version "7.1.0"
}

repositories {
  // Use Maven Central for resolving dependencies.
  mavenCentral()
}

dependencies {
  implementation(project(":spark"))

  // For a real Spark application, these would not be required since they would be in the Spark
  // server classpath
  runtimeOnly("org.apache.spark:spark-core_2.12:3.5.1")
  runtimeOnly("org.apache.spark:spark-hive_2.12:3.5.1")
}

tasks.jar {
  dependsOn(":spark:jar", ":core:jar", ":core:shadowJar")

  isZip64 = true
  exclude("META-INF/*.RSA")
  exclude("META-INF/*.SF")
  exclude("META-INF/*.DSA")

  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
  manifest.attributes["Main-Class"] = "io.substrait.examples.App"
  from(configurations.runtimeClasspath.get().map({ if (it.isDirectory) it else zipTree(it) }))
}

tasks.named<Test>("test") {
  // Use JUnit Platform for unit tests.
  useJUnitPlatform()
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }
