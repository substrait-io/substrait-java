import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import org.gradle.api.provider.ValueSource
import org.gradle.api.provider.ValueSourceParameters
import org.gradle.plugins.ide.idea.model.IdeaModel
import org.slf4j.LoggerFactory

plugins {
  `maven-publish`
  signing
  id("java-library")
  id("idea")
  id("antlr")
  id("com.google.protobuf") version "0.9.4"
  id("com.diffplug.spotless") version "6.19.0"
  id("com.gradleup.shadow") version "8.3.6"
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
}

configurations[JavaPlugin.API_CONFIGURATION_NAME].let { apiConfiguration ->
  // Workaround for https://github.com/gradle/gradle/issues/820
  apiConfiguration.setExtendsFrom(apiConfiguration.extendsFrom.filter { it.name != "antlr" })
}

abstract class SubstraitSpecVersionValueSource :
  ValueSource<String, SubstraitSpecVersionValueSource.Parameters> {
  companion object {
    val logger = LoggerFactory.getLogger("SubstraitSpecVersionValueSource")
  }

  interface Parameters : ValueSourceParameters {
    val substraitDirectory: Property<File>
  }

  @get:Inject abstract val execOperations: ExecOperations

  override fun obtain(): String {
    val stdOutput = ByteArrayOutputStream()
    val errOutput = ByteArrayOutputStream()
    execOperations.exec {
      commandLine("git", "describe", "--tags")
      standardOutput = stdOutput
      errorOutput = errOutput
      setIgnoreExitValue(true)
      workingDir = parameters.substraitDirectory.get()
    }

    // capturing the error output and logging it to avoid issues with VS Code Spotless plugin
    val error = String(errOutput.toByteArray())
    if (error != "") {
      logger.warn(error)
    }

    var cmdOut = String(stdOutput.toByteArray()).trim()

    if (cmdOut == "") {
      cmdOut = "0.0.0"
    } else if (cmdOut.startsWith("v")) {
      return cmdOut.substring(1)
    }

    return cmdOut
  }
}

tasks.register("writeManifest") {
  doLast {
    val substraitSpecVersionProvider =
      providers.of(SubstraitSpecVersionValueSource::class) {
        parameters.substraitDirectory.set(project(":").file("substrait"))
      }

    val manifestFile =
      layout.buildDirectory
        .file("generated/sources/manifest/META-INF/MANIFEST.MF")
        .get()
        .getAsFile()
    manifestFile.getParentFile().mkdirs()

    manifestFile.printWriter(StandardCharsets.UTF_8).use {
      it.println("Manifest-Version: 1.0")
      it.println("Implementation-Title: substrait-java")
      it.println("Implementation-Version: " + project.version)
      it.println("Specification-Title: substrait")
      it.println("Specification-Version: " + substraitSpecVersionProvider.get())
    }

    val substraitVersionClass =
      layout.buildDirectory
        .file("generated/sources/version/io/substrait/SubstraitVersion.java")
        .get()
        .getAsFile()
    substraitVersionClass.getParentFile().mkdirs()

    substraitVersionClass.printWriter(StandardCharsets.UTF_8).use {
      it.println("package io.substrait;\n")
      it.println("public class SubstraitVersion {")
      it.println(
        "  public static final String VERSION = \"" + substraitSpecVersionProvider.get() + "\";"
      )
      it.println("}")
    }
  }
}

tasks.named("compileJava") { dependsOn("writeManifest") }

tasks {
  shadowJar {
    archiveClassifier.set("") // to override ".jar" instead of producing "-all.jar"
    minimize()
    // bundle the deps from shadowImplementation into the jar
    configurations = listOf(shadowImplementation)
    // rename the shadowed deps so that they don't conflict with consumer's own deps
    relocate("org.antlr.v4.runtime", "io.substrait.org.antlr.v4.runtime")
  }

  jar { manifest { from("build/generated/sources/manifest/META-INF/MANIFEST.MF") } }
}

// Set the release instead of using a Java 8 toolchain since ANTLR requires Java 11+ to run
tasks.withType<JavaCompile>().configureEach { options.release = 8 }

java {
  withJavadocJar()
  withSourcesJar()
}

configurations { runtimeClasspath { resolutionStrategy.activateDependencyLocking() } }

tasks.named("sourcesJar") { mustRunAfter("generateGrammarSource") }

sourceSets {
  main {
    proto.srcDir("../substrait/proto")
    resources.srcDir("../substrait/extensions")
    resources.srcDir("build/generated/sources/manifest/")
    java.srcDir(file("build/generated/sources/antlr/main/java/"))
    java.srcDir("build/generated/sources/version/")
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
