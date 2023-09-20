import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc
import org.gradle.plugins.ide.idea.model.IdeaModel

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("antlr")
  id("com.google.protobuf") version "0.8.17"
  id("com.diffplug.spotless") version "6.11.0"
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

dependencies {
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.2")
  testImplementation("org.junit.jupiter:junit-jupiter-params:5.9.2")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
  implementation("com.google.protobuf:protobuf-java:3.17.3")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.13.4")
  implementation("com.fasterxml.jackson.core:jackson-annotations:2.13.4")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.13.4")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.13.4")
  implementation("com.google.code.findbugs:jsr305:3.0.2")

  antlr("org.antlr:antlr4:4.9.2")
  implementation("org.slf4j:slf4j-jdk14:1.7.30")
  implementation("org.antlr:antlr4-runtime:4.9.2")
  annotationProcessor("org.immutables:value:2.8.8")
  compileOnly("org.immutables:value-annotations:2.8.8")
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
}

java {
  toolchain {
    languageVersion.set(JavaLanguageVersion.of(17))
    withJavadocJar()
    withSourcesJar()
  }
}

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
  outputDirectory = File(buildDir, "generated/sources/antlr/main/java/io/substrait/type")
}

protobuf { protoc { artifact = "com.google.protobuf:protoc:3.17.3" } }
