import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.diffplug.spotless") version "6.11.0"
  id("com.github.johnrengelman.shadow") version "7.1.2"
  signing
}

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
            // TBD Get the list of
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
      val releasesRepoUrl = "$buildDir/repos/releases"
      val snapshotsRepoUrl = "$buildDir/repos/snapshots"
      url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
    }
  }
}

signing {
  setRequired({ gradle.taskGraph.hasTask("publishToSonatype") })
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

java {
  toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
  withJavadocJar()
  withSourcesJar()
}

var CALCITE_VERSION = properties.get("calcite.version")
var GUAVA_VERSION = properties.get("guava.version")
var IMMUTABLES_VERSION = properties.get("immutables.version")
var JACKSON_VERSION = properties.get("jackson.version")
var JUNIT_VERSION = properties.get("junit.version")
var SLF4J_VERSION = properties.get("slf4j.version")
var PROTOBUF_VERSION = properties.get("protobuf.version")

dependencies {
  implementation(project(":core"))
  implementation("org.apache.calcite:calcite-core:${CALCITE_VERSION}")
  implementation("org.apache.calcite:calcite-server:${CALCITE_VERSION}")
  testImplementation("org.junit.jupiter:junit-jupiter:${JUNIT_VERSION}")
  implementation("org.reflections:reflections:0.9.12")
  implementation("com.google.guava:guava:${GUAVA_VERSION}")
  implementation("com.fasterxml.jackson.core:jackson-databind:${JACKSON_VERSION}")
  implementation("com.fasterxml.jackson.core:jackson-annotations:${JACKSON_VERSION}")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:${JACKSON_VERSION}")
  implementation("com.google.protobuf:protobuf-java-util:${PROTOBUF_VERSION}") {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
  }
  implementation("com.google.code.findbugs:jsr305:3.0.2")
  implementation("com.github.ben-manes.caffeine:caffeine:3.0.4")
  implementation("org.immutables:value-annotations:${IMMUTABLES_VERSION}")
  implementation("org.slf4j:slf4j-api:${SLF4J_VERSION}")
  runtimeOnly("org.slf4j:slf4j-jdk14:${SLF4J_VERSION}")
  annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")
  testImplementation("org.apache.calcite:calcite-plus:${CALCITE_VERSION}")
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
}

tasks {
  named<ShadowJar>("shadowJar") {
    archiveBaseName.set("isthmus")
    manifest { attributes(mapOf("Main-Class" to "io.substrait.isthmus.PlanEntryPoint")) }
  }

  classes { dependsOn(":core:shadowJar") }
}

tasks { build { dependsOn(shadowJar) } }
