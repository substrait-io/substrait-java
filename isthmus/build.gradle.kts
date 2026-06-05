plugins {
  `maven-publish`
  signing
  id("java-library")
  id("idea")
  id("eclipse")
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
  toolchain { languageVersion = JavaLanguageVersion.of(17) }
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
    // calcite-core:1.41.0 has dependencies that contain vulnerabilities:
    // - CVE-2024-57699 (net.minidev:json-smart < 2.5.2)
    implementation(libs.json.smart)
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
  testRuntimeOnly(libs.slf4j.jdk14)
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
  shadowJar {
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

  build { dependsOn(shadowJar) }

  // Only set the compile release since JUnit 6 requires Java 17 to run tests.
  compileJava { options.release = 11 }
}

sourceSets { test { proto.srcDirs("src/test/resources/extensions") } }

protobuf { protoc { artifact = "com.google.protobuf:protoc:" + libs.protoc.get().getVersion() } }

tasks.named<Javadoc>("javadoc") {
  dependsOn(":core:javadoc", ":core:javadocProto")
  description = "Generate Javadoc for main sources."

  // Keep normal behavior for main javadoc (warnings allowed to show/fail if you want)
  options {
    require(this is StandardJavadocDocletOptions)
    encoding = "UTF-8"
    setDestinationDir(rootProject.layout.buildDirectory.dir("docs/${version}/isthmus").get().asFile)
    addStringOption("overview", "${rootProject.projectDir}/isthmus/src/main/javadoc/overview.html")
    addBooleanOption("Xdoclint:all", true)
    addBooleanOption("Xwerror", true)
    links("../core-proto/")
    links("../core/")
    links("https://calcite.apache.org/javadocAggregate/")
  }
}

// Bundle both passes into the Javadoc JAR used for publishing.
tasks.named<Jar>("javadocJar") {
  // auto creates the directories if needed
  val docsDir = rootProject.layout.buildDirectory.dir("docs/${version}")
  destinationDirectory.set(docsDir)

  from(tasks.named("javadoc"))
  // Ensure javadoc tasks have produced output
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
