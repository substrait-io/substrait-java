// Define build variants
data class SparkVariant(
  val sparkVersion: String,
  val scalaVersion: String,
  val sparkMajorMinor: String,
  val scalaBinary: String,
  val classifier: String,
)

val sparkVariants =
  listOf(
    SparkVariant("3.4.4", "2.12.20", "3.4", "2.12", "spark34_2.12"),
    SparkVariant("3.5.4", "2.12.20", "3.5", "2.12", "spark35_2.12"),
    SparkVariant("4.0.2", "2.13.18", "4.0", "2.13", "spark40_2.13"),
  )

// Determine which variant to build (default to Spark 4.0)
val buildVariant = project.findProperty("sparkVariant")?.toString() ?: "spark40_2.13"
val currentVariant =
  sparkVariants.find { it.classifier == buildVariant }
    ?: throw GradleException(
      "Unknown variant: $buildVariant. Available: ${sparkVariants.map { it.classifier }}"
    )

println(
  "Building Spark variant: ${currentVariant.classifier} (Spark ${currentVariant.sparkVersion}, Scala ${currentVariant.scalaVersion})"
)

plugins {
  `maven-publish`
  signing
  id("java-library")
  id("scala")
  id("idea")
  alias(libs.plugins.spotless)
  alias(libs.plugins.nmcp)
  id("substrait.java-conventions")
}

val stagingRepositoryUrl = uri(layout.buildDirectory.dir("staging-deploy"))

publishing {
  publications {
    create<MavenPublication>("maven-publish") {
      from(components["java"])

      artifactId = "substrait-spark-${currentVariant.classifier}"

      pom {
        name.set("Substrait Spark ${currentVariant.classifier}")
        description.set(
          "Substrait integration for Apache Spark ${currentVariant.sparkMajorMinor} with Scala ${currentVariant.scalaBinary}"
        )
        url.set("https://github.com/substrait-io/substrait-java")

        properties.put("spark.version", currentVariant.sparkVersion)
        properties.put("scala.version", currentVariant.scalaVersion)
        properties.put("scala.binary.version", currentVariant.scalaBinary)

        licenses {
          license {
            name.set("The Apache License, Version 2.0")
            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
          }
        }
        developers {
          developer {
            id = "vbarua"
            name = "Victor Barua"
          }
        }
        scm {
          connection.set("scm:git:git://github.com:substrait-io/substrait-java.git")
          developerConnection.set("scm:git:ssh://github.com:substrait-io/substrait-java")
          url.set("https://github.com/substrait-io/substrait-java/")
        }
      }
    }
  }
  repositories {
    maven {
      name = "local"
      val releasesRepoUrl = layout.buildDirectory.dir("repos/releases")
      val snapshotsRepoUrl = layout.buildDirectory.dir("repos/snapshots")
      url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else snapshotsRepoUrl)
    }
  }
}

signing {
  setRequired({
    gradle.taskGraph.hasTask(":${project.name}:publishMaven-publishPublicationToNmcpRepository")
  })
  val signingKeyId =
    System.getenv("SIGNING_KEY_ID").takeUnless { it.isNullOrEmpty() }
      ?: extra["SIGNING_KEY_ID"].toString()
  val signingPassword =
    System.getenv("SIGNING_PASSWORD").takeUnless { it.isNullOrEmpty() }
      ?: extra["SIGNING_PASSWORD"].toString()
  val signingKey =
    System.getenv("SIGNING_KEY").takeUnless { it.isNullOrEmpty() }
      ?: extra["SIGNING_KEY"].toString()
  useInMemoryPgpKeys(signingKeyId, signingKey, signingPassword)
  sign(publishing.publications["maven-publish"])
}

configurations.all {
  if (name.startsWith("incrementalScalaAnalysis")) {
    setExtendsFrom(emptyList())
  }
}

// Java toolchain is configured by substrait.java-conventions plugin
// withJavadocJar() and withSourcesJar() are also configured there

tasks.withType<ScalaCompile>() {
  scalaCompileOptions.additionalParameters = listOf("-release:17", "-Xfatal-warnings")
}

var SPARKBUNDLE_VERSION = properties.get("sparkbundle.version")

sourceSets {
  main {
    scala {
      setSrcDirs(listOf("src/main/scala", "src/main/spark-${currentVariant.sparkMajorMinor}"))
    }
  }
  test {
    scala {
      setSrcDirs(
        listOf(
          "src/test/scala",
          "src/test/spark-${currentVariant.sparkMajorMinor}",
          "src/main/scala",
        )
      )
    }
  }
}

dependencies {
  api(project(":core"))

  // Scala dependencies based on variant
  when (currentVariant.scalaBinary) {
    "2.12" -> {
      implementation("org.scala-lang:scala-library:${currentVariant.scalaVersion}")
      testImplementation("org.scalatest:scalatest_2.12:3.2.19")
      testRuntimeOnly("org.scalatestplus:junit-5-12_2.12:3.2.19.0")
    }
    "2.13" -> {
      implementation("org.scala-lang:scala-library:${currentVariant.scalaVersion}")
      testImplementation("org.scalatest:scalatest_2.13:3.2.19")
      testRuntimeOnly("org.scalatestplus:junit-5-13_2.13:3.2.19.0")
    }
  }

  // Spark dependencies based on variant
  when (currentVariant.classifier) {
    "spark34_2.12" -> {
      api("org.apache.spark:spark-core_2.12:${currentVariant.sparkVersion}")
      api("org.apache.spark:spark-sql_2.12:${currentVariant.sparkVersion}")
      implementation("org.apache.spark:spark-hive_2.12:${currentVariant.sparkVersion}")
      implementation("org.apache.spark:spark-catalyst_2.12:${currentVariant.sparkVersion}")
      testImplementation("org.apache.spark:spark-core_2.12:${currentVariant.sparkVersion}:tests")
      testImplementation("org.apache.spark:spark-sql_2.12:${currentVariant.sparkVersion}:tests")
      testImplementation(
        "org.apache.spark:spark-catalyst_2.12:${currentVariant.sparkVersion}:tests"
      )
    }
    "spark35_2.12" -> {
      api("org.apache.spark:spark-core_2.12:${currentVariant.sparkVersion}")
      api("org.apache.spark:spark-sql_2.12:${currentVariant.sparkVersion}")
      implementation("org.apache.spark:spark-hive_2.12:${currentVariant.sparkVersion}")
      implementation("org.apache.spark:spark-catalyst_2.12:${currentVariant.sparkVersion}")
      testImplementation("org.apache.spark:spark-core_2.12:${currentVariant.sparkVersion}:tests")
      testImplementation("org.apache.spark:spark-sql_2.12:${currentVariant.sparkVersion}:tests")
      testImplementation(
        "org.apache.spark:spark-catalyst_2.12:${currentVariant.sparkVersion}:tests"
      )
    }
    "spark40_2.13" -> {
      api("org.apache.spark:spark-core_2.13:${currentVariant.sparkVersion}")
      api("org.apache.spark:spark-sql_2.13:${currentVariant.sparkVersion}")
      implementation("org.apache.spark:spark-hive_2.13:${currentVariant.sparkVersion}")
      implementation("org.apache.spark:spark-catalyst_2.13:${currentVariant.sparkVersion}")
      testImplementation("org.apache.spark:spark-core_2.13:${currentVariant.sparkVersion}:tests")
      testImplementation("org.apache.spark:spark-sql_2.13:${currentVariant.sparkVersion}:tests")
      testImplementation(
        "org.apache.spark:spark-catalyst_2.13:${currentVariant.sparkVersion}:tests"
      )
    }
  }

  // Common dependencies (keep existing)
  implementation(libs.slf4j.api)
  implementation(platform(libs.jackson.bom))
  implementation(libs.bundles.jackson)
  implementation(libs.json.schema.validator)

  testImplementation(platform(libs.junit.bom))
  testRuntimeOnly(libs.junit.platform.engine)
  testRuntimeOnly(libs.junit.platform.launcher)
}

spotless {
  scala {
    scalafmt().configFile(".scalafmt.conf")
    toggleOffOn()
  }
}

tasks.register<JavaExec>("dialect") {
  dependsOn(":core:shadowJar")
  classpath = java.sourceSets["main"].runtimeClasspath
  mainClass = "io.substrait.spark.utils.DialectGenerator"
  args = listOf("spark_dialect.yaml")
}

tasks {
  jar {
    manifest {
      from("../core/build/generated/sources/manifest/META-INF/MANIFEST.MF")
      attributes("Implementation-Title" to "substrait-spark")
    }
  }

  test {
    dependsOn(":core:shadowJar")
    useJUnitPlatform { includeEngines("scalatest") }

    // Set system properties for variant identification
    systemProperty("spark.version", currentVariant.sparkVersion)
    systemProperty("scala.version", currentVariant.scalaVersion)
    systemProperty("variant.classifier", currentVariant.classifier)

    jvmArgs(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    )
    environment("SPARK_LOCAL_IP", "127.0.0.1")

    // Separate test reports per variant
    reports {
      html.outputLocation.set(
        layout.buildDirectory.dir("reports/tests/${currentVariant.classifier}")
      )
      junitXml.outputLocation.set(
        layout.buildDirectory.dir("test-results/${currentVariant.classifier}")
      )
    }
  }
}

// Task to list all available variants
tasks.register("listVariants") {
  group = "help"
  description = "Lists all available Spark/Scala build variants"

  doLast {
    println("\nAvailable Spark/Scala variants:")
    println("================================")
    sparkVariants.forEach { variant ->
      println("  • ${variant.classifier}")
      println("    - Spark: ${variant.sparkVersion} (${variant.sparkMajorMinor})")
      println("    - Scala: ${variant.scalaVersion} (${variant.scalaBinary})")
      println()
    }
    println("Usage:")
    println("  Build specific variant: ./gradlew :spark:build -PsparkVariant=<classifier>")
    println("  Build all variants:     ./gradlew :spark:buildAllVariants")
    println("  Publish all variants:   ./gradlew :spark:publishAllVariants")
    println()
  }
}
