plugins {
  // Apply the application plugin to add support for building a CLI application in Java.
  id("java")
  alias(libs.plugins.spotless)
  id("substrait.java-conventions")
}

repositories {
  // Use Maven Central for resolving dependencies.
  mavenCentral()
}

// Get the Spark variant property
val sparkVariantProp = findProperty("sparkVariant")?.toString() ?: "spark40_2.13"

// Map variants to their versions
val variantVersions =
  mapOf(
    "spark34_2.12" to Pair("3.4.4", "2.12"),
    "spark35_2.12" to Pair("3.5.3", "2.12"),
    "spark40_2.13" to Pair("4.0.2", "2.13"),
  )

val (sparkVersion, scalaBinary) =
  variantVersions[sparkVariantProp] ?: variantVersions["spark40_2.13"]!!

dependencies {
  implementation(project(":spark"))

  // For a real Spark application, these would not be required since they would be in the Spark
  // server classpath. Use direct Maven coordinates to match the spark module's variant.
  runtimeOnly("org.apache.spark:spark-core_${scalaBinary}:${sparkVersion}")
  runtimeOnly("org.apache.spark:spark-hive_${scalaBinary}:${sparkVersion}")
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

tasks.pmdMain { dependsOn(":core:shadowJar") }
