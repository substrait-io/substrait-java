import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  signing
  id("java-library")
  id("idea")
  alias(libs.plugins.shadow)
  alias(libs.plugins.spotless)
  alias(libs.plugins.protobuf)
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

java {
  toolchain { languageVersion = JavaLanguageVersion.of(11) }
  withJavadocJar()
  withSourcesJar()
}

configurations { runtimeClasspath { resolutionStrategy.activateDependencyLocking() } }

dependencies {
  api(project(":core"))
  api(libs.calcite.core) {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  constraints {
    // calcite-core:1.39.0 has dependencies that contain vulnerabilities:
    // - CVE-2025-27820 (org.apache.httpcomponents.client5:httpclient5 < 5.4.3)
    // - CVE-2024-57699 (net.minidev:json-smart < 2.5.2)
    implementation(libs.httpclient5)
    implementation(libs.json.smart)
    // calcite-core:1.40.0 has dependencies that contain vulnerabilities:
    // - CVE-2025-48924 (org.apache.commons:commons-lang3 < 3.18.0)
    implementation(libs.commons.lang3)
  }
  implementation(libs.calcite.server) {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)
  implementation(libs.guava)
  implementation(libs.protobuf.java.util) {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
  }
  annotationProcessor(libs.immutables.value)
  compileOnly(libs.immutables.annotations)
  implementation(libs.slf4j.api)
  testImplementation(libs.calcite.plus) {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  testImplementation(libs.protobuf.java)
  api(libs.jspecify)
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

protobuf { protoc { artifact = "com.google.protobuf:protoc:" + libs.protoc.get().getVersion() } }
