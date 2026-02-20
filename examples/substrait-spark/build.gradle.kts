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

// Get the Spark variant property - determines which spark subproject to use
val sparkVariantProp = findProperty("sparkVariant")?.toString() ?: "spark40_2.13"

// Map variants to their subproject paths and versions
val variantConfig =
  mapOf(
    "spark34_2.12" to Triple(":spark:spark-3.4_2.12", "3.4.4", "2.12"),
    "spark35_2.12" to Triple(":spark:spark-3.5_2.12", "3.5.4", "2.12"),
    "spark40_2.13" to Triple(":spark:spark-4.0_2.13", "4.0.2", "2.13"),
  )

val (sparkProject, sparkVersion, scalaBinary) =
  variantConfig[sparkVariantProp] ?: variantConfig["spark40_2.13"]!!

dependencies {
  // Depend on the specific spark variant subproject
  implementation(project(sparkProject))

  // For a real Spark application, these would not be required since they would be in the Spark
  // server classpath. Use direct Maven coordinates to match the spark module's variant.
  runtimeOnly("org.apache.spark:spark-core_${scalaBinary}:${sparkVersion}")
  runtimeOnly("org.apache.spark:spark-hive_${scalaBinary}:${sparkVersion}")
}

tasks.jar {
  dependsOn("$sparkProject:jar", ":core:jar", ":core:shadowJar")

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
