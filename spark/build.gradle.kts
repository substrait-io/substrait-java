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

      pom {
        name.set("Substrait Java")
        description.set(
          "Create a well-defined, cross-language specification for data compute operations"
        )
        url.set("https://github.com/substrait-io/substrait-java")
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

var SPARKBUNDLE_VERSION = properties.get("sparkbundle.version")

sourceSets {
  main { scala { setSrcDirs(listOf("src/main/scala", "src/main/spark-3.4")) } }
  test { scala { setSrcDirs(listOf("src/test/scala", "src/test/spark-3.2", "src/main/scala")) } }
}

dependencies {
  api(project(":core"))
  implementation(libs.scala.library)
  api(libs.spark.core)
  api(libs.spark.sql)
  implementation(libs.spark.hive)
  implementation(libs.spark.catalyst)
  implementation(libs.slf4j.api)
  implementation(platform(libs.jackson.bom))
  implementation(libs.bundles.jackson)
  implementation(libs.json.schema.validator)

  testImplementation(libs.scalatest)
  testImplementation(platform(libs.junit.bom))
  testRuntimeOnly(libs.junit.platform.engine)
  testRuntimeOnly(libs.junit.platform.launcher)
  testRuntimeOnly(libs.scalatestplus.junit5)

  testImplementation(variantOf(libs.spark.core) { classifier("tests") })
  testImplementation(variantOf(libs.spark.sql) { classifier("tests") })
  testImplementation(variantOf(libs.spark.catalyst) { classifier("tests") })
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
    jvmArgs(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
    )
  }
}
