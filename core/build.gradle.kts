import java.nio.charset.StandardCharsets
import org.gradle.plugins.ide.idea.model.IdeaModel

plugins {
  `maven-publish`
  signing
  id("java-library")
  id("idea")
  id("eclipse")
  alias(libs.plugins.spotless)
  alias(libs.plugins.shadow)
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

// This allows specifying deps to be shadowed so that they don't get included in the POM file
val shadowImplementation = configurations.create("shadowImplementation")

configurations[JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME].extendsFrom(shadowImplementation)

configurations[JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME].extendsFrom(shadowImplementation)

dependencies {
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.protobuf.java.util)
  testImplementation(libs.guava)
  testImplementation(libs.bundles.jackson)
  testImplementation(libs.classgraph)
  testImplementation(libs.json.schema.validator)

  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)

  implementation(platform(libs.jackson.bom))
  implementation(libs.bundles.jackson)

  // Compiled protobuf bindings (io.substrait.proto) from the substrait-packaging artifact,
  // which transitively brings protobuf-java.
  api(libs.substrait.protobuf)
  api(libs.jspecify)

  // Compiled ANTLR parsers (io.substrait.antlr) from the substrait-packaging artifact.
  // It is shadowed so the ANTLR runtime it pulls in is relocated (see shadowJar below) and
  // does not leak onto consumers' classpaths. Exclude the ANTLR tool (antlr4): only the
  // runtime is needed to run the generated parsers.
  shadowImplementation(libs.substrait.antlr) { exclude(group = "org.antlr", module = "antlr4") }
  // Extension YAMLs, text schemas and function test cases (under substrait/ on the classpath).
  implementation(libs.substrait.extensions)
  implementation(libs.slf4j.api)
  annotationProcessor(libs.immutables.value)
  compileOnly(libs.immutables.annotations)
}

// The Substrait spec version is the version of the substrait-packaging artifacts consumed
// (see the version catalog), with any -SNAPSHOT suffix stripped. This is the spec release the
// generated proto/ANTLR/extension resources come from, so it is the single source of truth for
// SubstraitVersion and the manifest's Specification-Version.
val substraitSpecVersion = libs.versions.substrait.packaging.get().removeSuffix("-SNAPSHOT")

tasks.register("writeManifest") {
  val version = project.version
  val specVersion = substraitSpecVersion
  doLast {
    val manifestFile =
      layout.buildDirectory
        .file("generated/sources/manifest/META-INF/MANIFEST.MF")
        .get()
        .getAsFile()
    manifestFile.getParentFile().mkdirs()

    manifestFile.printWriter(StandardCharsets.UTF_8).use {
      it.println("Manifest-Version: 1.0")
      it.println("Implementation-Title: substrait-java")
      it.println("Implementation-Version: " + version)
      it.println("Specification-Title: substrait")
      it.println("Specification-Version: " + specVersion)
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
      it.println("  public static final String VERSION = \"" + specVersion + "\";")
      it.println("}")
    }
  }
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

  jar { manifest { from("build/generated/sources/manifest/META-INF/MANIFEST.MF") } }

  // Set the release instead of using a Java 8 toolchain since ANTLR requires Java 11+ to run.
  // Only set the compile release since JUnit 6 requires Java 17 to run tests.
  compileJava {
    options.release = 8
    dependsOn("writeManifest")
  }
}

java {
  toolchain { languageVersion = JavaLanguageVersion.of(17) }
  withJavadocJar()
  withSourcesJar()
}

configurations { runtimeClasspath { resolutionStrategy.activateDependencyLocking() } }

tasks.named<Jar>("sourcesJar") { duplicatesStrategy = DuplicatesStrategy.EXCLUDE }

sourceSets {
  main {
    resources.srcDir("build/generated/sources/manifest/")
    java.srcDir("build/generated/sources/version/")
  }
}

tasks.named<ProcessResources>("processTestResources") {
  // A real-world dialect to exercise parsing against. The dialect schema, the per-section
  // dialect fixtures and the extension YAMLs are on the test classpath from the
  // substrait-packaging extensions artifact under substrait/.
  from("../spark/spark_dialect.yaml") { into("dialect") }
}

project.configure<IdeaModel> {
  module { generatedSourceDirs.addAll(listOf(file("build/generated/sources/version"))) }
}

val immuteableJavaDir = layout.buildDirectory.dir("generated/sources/annotationProcessor/java/main")

tasks.register<Javadoc>("javadocImmutable") {
  dependsOn("compileJava", "javadoc")

  group = JavaBasePlugin.DOCUMENTATION_GROUP
  description = "Generate Javadoc for immutable-generated sources (warnings suppressed)."

  // Only the Immutables-generated sources
  setSource(fileTree(immuteableJavaDir) { include("**/*.java") })

  // Use the main source set classpath + compiled output to resolve types referenced by the
  // generated code (the Immutables-generated sources import the hand-written enclosing types,
  // which live in the project's own output, not in external dependency JARs).
  classpath = sourceSets["main"].compileClasspath + sourceSets["main"].output.classesDirs

  // Destination separate from main Javadoc
  setDestinationDir(rootProject.layout.buildDirectory.dir("docs/${version}/immutable").get().asFile)

  // Suppress warnings/doclint for the immutable pass
  options {
    require(this is StandardJavadocDocletOptions)
    addBooleanOption("Xdoclint:all", true)
    addBooleanOption("Xwerror", true)
    // Encoding is good practice
    encoding = "UTF-8"
    addStringOption(
      "overview",
      "${rootProject.projectDir}/core/src/main/javadoc/overview-immutable.html",
    )
    links("../core/")
  }
}

// Javadoc for main code. Only the version-generated directory needs excluding from the
// hand-written pass; proto and ANTLR classes come compiled from the substrait-packaging artifacts.
tasks.named<Javadoc>("javadoc") {
  description = "Generate Javadoc for main sources."

  // Exclude the version-generated directory from the main pass. These sources are regenerated
  // on every build and cannot carry hand-written Javadoc.
  val generatedDirs =
    listOf(layout.buildDirectory.dir("generated/sources/version")).map { it.get().asFile.toPath() }
  exclude { spec -> generatedDirs.any { spec.file.toPath().startsWith(it) } }
  source(fileTree(immuteableJavaDir) { include("**/*.java") })

  // Fail the build if Javadoc linting finds any issues in the hand-written sources.
  options {
    require(this is StandardJavadocDocletOptions)
    addBooleanOption("Xdoclint:all", true)
    addBooleanOption("Xwerror", true)
    encoding = "UTF-8"
    setDestinationDir(rootProject.layout.buildDirectory.dir("docs/${version}/core").get().asFile)
    addStringOption("overview", "${rootProject.projectDir}/core/src/main/javadoc/overview.html")
  }
}

// Bundle both passes into the Javadoc JAR used for publishing.
tasks.named<Jar>("javadocJar") {
  // auto creates the directories if needed
  val docsDir = rootProject.layout.buildDirectory.dir("docs/${version}")
  destinationDirectory.set(docsDir)

  // Add the outputs of the Javadoc tasks to this JAR
  // Using 'from' on a task automatically adds the 'dependsOn'
  from(tasks.named("javadoc"))
  from(tasks.named("javadocImmutable"))

  // Handle duplicate files (e.g., allclasses-index.html) from multiple javadoc tasks
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

// workaround for Eclipse/VS Code bug handling annotationProcessor sources
// https://github.com/redhat-developer/vscode-java/issues/2981
eclipse {
  classpath {
    containers("org.eclipse.buildship.core.gradleclasspathcontainer")
    file.whenMerged {
      if (this is org.gradle.plugins.ide.eclipse.model.Classpath) {
        entries.add(
          org.gradle.plugins.ide.eclipse.model.SourceFolder(
            "build/generated/sources/annotationProcessor/java/main",
            null,
          )
        )
      }
    }
  }
}
