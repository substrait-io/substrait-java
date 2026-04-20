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

// Spark 4.0 with Scala 2.13 variant configuration
val sparkVersion = "4.0.2"
val scalaVersion = "2.13.18"
val sparkMajorMinor = "4.0"
val scalaBinary = "2.13"
val classifier = "spark40_2.13"

publishing {
  publications {
    create<MavenPublication>("maven-publish") {
      from(components["java"])

      artifactId = classifier

      pom {
        name.set("Substrait Spark $classifier")
        description.set(
          "Substrait integration for Apache Spark $sparkMajorMinor with Scala $scalaBinary"
        )
        url.set("https://github.com/substrait-io/substrait-java")

        properties.put("spark.version", sparkVersion)
        properties.put("scala.version", scalaVersion)
        properties.put("scala.binary.version", scalaBinary)

        licenses {
          license {
            name.set("The Apache License, Version 2.0")
            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
          }
        }
        developers {
          developer {
            id = "andrew-coleman"
            name = "Andrew Coleman"
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
      url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
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

java {
  toolchain { languageVersion = JavaLanguageVersion.of(17) }
  withJavadocJar()
  withSourcesJar()
}

tasks.withType<ScalaCompile>() {
  scalaCompileOptions.additionalParameters = listOf("-release:17", "-Xfatal-warnings")
}

// Configure source sets to reference parent's src directory
sourceSets {
  main {
    scala { setSrcDirs(listOf("../src/main/scala", "../src/main/spark-$sparkMajorMinor")) }
    resources { setSrcDirs(listOf("../src/main/resources")) }
  }
  test {
    scala {
      setSrcDirs(
        listOf("../src/test/scala", "../src/test/spark-$sparkMajorMinor", "../src/main/scala")
      )
    }
    resources { setSrcDirs(listOf("../src/test/resources")) }
  }
}

dependencies {
  // Use runtimeElements configuration to avoid test classpath resolution
  api(project(":core", configuration = "runtimeElements"))

  // Scala dependencies
  implementation("org.scala-lang:scala-library:$scalaVersion")
  testImplementation("org.scalatest:scalatest_$scalaBinary:3.2.20")
  testRuntimeOnly("org.scalatestplus:junit-5-14_$scalaBinary:3.2.20.0")

  // Spark dependencies
  api("org.apache.spark:spark-core_$scalaBinary:$sparkVersion")
  api("org.apache.spark:spark-sql_$scalaBinary:$sparkVersion")
  implementation("org.apache.spark:spark-hive_$scalaBinary:$sparkVersion")
  implementation("org.apache.spark:spark-catalyst_$scalaBinary:$sparkVersion")
  testImplementation("org.apache.spark:spark-core_$scalaBinary:$sparkVersion:tests")
  testImplementation("org.apache.spark:spark-sql_$scalaBinary:$sparkVersion:tests")
  testImplementation("org.apache.spark:spark-catalyst_$scalaBinary:$sparkVersion:tests")

  // Common dependencies
  implementation(libs.slf4j.api)
  implementation(platform(libs.jackson.bom))
  implementation(libs.bundles.jackson)
  implementation(libs.json.schema.validator)

  testImplementation(platform(libs.junit.bom))
  testRuntimeOnly(libs.junit.platform.engine)
  testRuntimeOnly(libs.junit.platform.launcher)
}

// Spotless is disabled for subprojects since source files are in parent directory
// The parent spark project handles formatting
spotless { isEnforceCheck = false }

tasks.register<JavaExec>("dialect") {
  dependsOn(":core:shadowJar")
  classpath = java.sourceSets["main"].runtimeClasspath
  mainClass = "io.substrait.spark.utils.DialectGenerator"
  args = listOf("../spark_dialect.yaml")
}

tasks {
  // Ensure shadowJar runs before compilation
  named("compileJava") { dependsOn(":core:shadowJar") }
  named("compileScala") { dependsOn(":core:shadowJar") }

  jar {
    manifest {
      from("../../core/build/generated/sources/manifest/META-INF/MANIFEST.MF")
      attributes("Implementation-Title" to "substrait-spark-$classifier")
    }
  }

  test {
    dependsOn(":core:shadowJar")
    useJUnitPlatform { includeEngines("scalatest") }

    // Set system properties for variant identification
    systemProperty("spark.version", sparkVersion)
    systemProperty("scala.version", scalaVersion)
    systemProperty("variant.classifier", classifier)

    jvmArgs(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
    )
    environment("SPARK_LOCAL_IP", "127.0.0.1")

    // Separate test reports per variant
    reports {
      html.outputLocation.set(layout.buildDirectory.dir("reports/tests/$classifier"))
      junitXml.outputLocation.set(layout.buildDirectory.dir("test-results/$classifier"))
    }
  }
}
