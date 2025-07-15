import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  signing
  id("java-library")
  id("idea")
  id("com.diffplug.spotless") version "6.19.0"
  id("com.gradleup.shadow") version "8.3.6"
  id("com.google.protobuf") version "0.9.4"
  id("org.jreleaser")
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
    maven {
      name = "staging"
      url = stagingRepositoryUrl
    }
  }
}

signing {
  setRequired({
    gradle.taskGraph.hasTask(":${project.name}:publishMaven-publishPublicationToStagingRepository")
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

jreleaser {
  gitRootSearch = true
  deploy {
    maven {
      mavenCentral {
        register("sonatype") {
          active = org.jreleaser.model.Active.ALWAYS
          url = "https://central.sonatype.com/api/v1/publisher"
          sign = false
          stagingRepository(file(stagingRepositoryUrl).toString())
        }
      }
    }
  }
  release { github { enabled = false } }
}

java {
  toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
  withJavadocJar()
  withSourcesJar()
}

configurations { runtimeClasspath { resolutionStrategy.activateDependencyLocking() } }

val CALCITE_VERSION = properties.get("calcite.version")
val GUAVA_VERSION = properties.get("guava.version")
val IMMUTABLES_VERSION = properties.get("immutables.version")
val JACKSON_VERSION = properties.get("jackson.version")
val JUNIT_VERSION = properties.get("junit.version")
val SLF4J_VERSION = properties.get("slf4j.version")
val PROTOBUF_VERSION = properties.get("protobuf.version")

dependencies {
  api(project(":core"))
  api("org.apache.calcite:calcite-core:${CALCITE_VERSION}") {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  constraints {
    // calcite-core:1.39.0 has dependencies that contain vulnerabilities:
    // - CVE-2025-27820 (org.apache.httpcomponents.client5:httpclient5 < 5.4.3)
    // - CVE-2024-57699 (net.minidev:json-smart < 2.5.2)
    implementation("org.apache.httpcomponents.client5:httpclient5:5.4.4")
    implementation("net.minidev:json-smart:2.5.2")
    // calcite-core:1.40.0 has dependencies that contain vulnerabilities:
    // - CVE-2025-48924 (org.apache.commons:commons-lang3 < 3.18.0)
    implementation("org.apache.commons:commons-lang3:[3.18.0,)")
  }
  implementation("org.apache.calcite:calcite-server:${CALCITE_VERSION}") {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  testImplementation(platform("org.junit:junit-bom:${JUNIT_VERSION}"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
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
  annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")
  testImplementation("org.apache.calcite:calcite-plus:${CALCITE_VERSION}") {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  testImplementation("com.google.protobuf:protobuf-java:${PROTOBUF_VERSION}")
}

tasks {
  named<ShadowJar>("shadowJar") {
    archiveBaseName.set("isthmus")
    manifest { attributes(mapOf("Main-Class" to "io.substrait.isthmus.PlanEntryPoint")) }
  }

  classes { dependsOn(":core:shadowJar") }

  jar {
    manifest {
      from("../core/build/generated/sources/manifest/META-INF/MANIFEST.MF")
      attributes("Implementation-Title" to "isthmus")
    }
  }
}

tasks { build { dependsOn(shadowJar) } }

sourceSets { test { proto.srcDirs("src/test/resources/extensions") } }

protobuf { protoc { artifact = "com.google.protobuf:protoc:${PROTOBUF_VERSION}" } }
