import org.gradle.plugins.ide.idea.model.IdeaModel

plugins {
  `maven-publish`
  id("java-library")
  id("idea")
  id("antlr")
  id("com.google.protobuf") version "0.9.4"
  id("com.diffplug.spotless") version "6.19.0"
  id("com.gradleup.shadow") version "8.3.6"
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
      val releasesRepoUrl = layout.buildDirectory.dir("repos/releases")
      val snapshotsRepoUrl = layout.buildDirectory.dir("repos/snapshots")
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

val ANTLR_VERSION = properties.get("antlr.version")
val IMMUTABLES_VERSION = properties.get("immutables.version")
val JACKSON_VERSION = properties.get("jackson.version")
val JUNIT_VERSION = properties.get("junit.version")
val SLF4J_VERSION = properties.get("slf4j.version")
val PROTOBUF_VERSION = properties.get("protobuf.version")

// This allows specifying deps to be shadowed so that they don't get included in the POM file
val shadowImplementation by configurations.creating

configurations[JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME].extendsFrom(shadowImplementation)

configurations[JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME].extendsFrom(shadowImplementation)

dependencies {
  testImplementation(platform("org.junit:junit-bom:${JUNIT_VERSION}"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  api("com.google.protobuf:protobuf-java:${PROTOBUF_VERSION}")
  implementation("com.fasterxml.jackson.core:jackson-databind:${JACKSON_VERSION}")
  implementation("com.fasterxml.jackson.core:jackson-annotations:${JACKSON_VERSION}")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:${JACKSON_VERSION}")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${JACKSON_VERSION}")
  implementation("com.google.code.findbugs:jsr305:3.0.2")

  antlr("org.antlr:antlr4:${ANTLR_VERSION}")
  shadowImplementation("org.antlr:antlr4-runtime:${ANTLR_VERSION}")
  implementation("org.slf4j:slf4j-api:${SLF4J_VERSION}")
  annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")
  compileOnly("org.immutables:value-annotations:${IMMUTABLES_VERSION}")
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
}

configurations[JavaPlugin.API_CONFIGURATION_NAME].let { apiConfiguration ->
  // Workaround for https://github.com/gradle/gradle/issues/820
  apiConfiguration.setExtendsFrom(apiConfiguration.extendsFrom.filter { it.name != "antlr" })
}

tasks {
  shadowJar {
    archiveClassifier.set("") // to override ".jar" instead of producing "-all.jar"
    minimize()
    // bundle the deps from shadowImplementation into the jar
    configurations = listOf(shadowImplementation)
    // rename the shadowed deps so that they don't conflict with consumer's own deps
    relocate("org.antlr.v4.runtime", "io.substrait.org.antlr.v4.runtime")
  }
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(17))
    withJavadocJar()
    withSourcesJar()
  }
}

configurations { runtimeClasspath { resolutionStrategy.activateDependencyLocking() } }

tasks.named("sourcesJar") { mustRunAfter("generateGrammarSource") }

sourceSets {
  main {
    proto.srcDir("../substrait/proto")
    resources.srcDir("../substrait/extensions")
    java.srcDir(file("build/generated/sources/antlr/main/java/"))
  }
}

project.configure<IdeaModel> {
  module {
    resourceDirs.addAll(
      listOf(file("../substrait/text"), file("../substrait/extensions"), file("../substrait/proto"))
    )
    generatedSourceDirs.addAll(
      listOf(
        file("build/generated/sources/antlr/main"),
        file("build/generated/source/proto/main/java")
      )
    )
  }
}

tasks.named<AntlrTask>("generateGrammarSource") {
  arguments.add("-package")
  arguments.add("io.substrait.type")
  arguments.add("-visitor")
  arguments.add("-long-messages")
  arguments.add("-Xlog")
  arguments.add("-Werror")
  arguments.add("-Xexact-output-dir")
  setSource(fileTree("src/main/antlr/SubstraitType.g4"))
  outputDirectory =
    layout.buildDirectory.dir("generated/sources/antlr/main/java/io/substrait/type").get().asFile
}

protobuf { protoc { artifact = "com.google.protobuf:protoc:${PROTOBUF_VERSION}" } }
