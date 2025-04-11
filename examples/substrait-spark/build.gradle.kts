plugins {
  // Apply the application plugin to add support for building a CLI application in Java.
  id("java")
  id("com.diffplug.spotless") version "6.19.0"
}

repositories {
  // Use Maven Central for resolving dependencies.
  mavenCentral()
}

var SPARK_VERSION = properties.get("spark.version")

dependencies {
  implementation("org.apache.spark:spark-core_2.12:${SPARK_VERSION}")
  implementation(project(":spark"))
  implementation(project(":core"))
  implementation("org.apache.spark:spark-sql_2.12:${SPARK_VERSION}")

  // For a real Spark application, these would not be required since they would be in the Spark
  // server classpath
  runtimeOnly("org.apache.spark:spark-core_2.12:${SPARK_VERSION}")
  runtimeOnly("org.apache.spark:spark-hive_2.12:${SPARK_VERSION}")
}

tasks.jar {
  isZip64 = true
  exclude("META-INF/*.RSA")
  exclude("META-INF/*.SF")
  exclude("META-INF/*.DSA")

  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
  manifest.attributes["Main-Class"] = "io.substrait.examples.App"
  from(configurations.runtimeClasspath.get().map({ if (it.isDirectory) it else zipTree(it) }))

  dependsOn(":core:shadowJar", ":core:jar")
}

tasks.named<Test>("test") {
  // Use JUnit Platform for unit tests.
  useJUnitPlatform()
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }
