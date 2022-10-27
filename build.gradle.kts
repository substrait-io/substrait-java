import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.github.vlsi.gradle.dsl.configureEach

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.github.vlsi.gradle-extensions") version "1.74"
  id("com.diffplug.spotless") version "6.11.0"
}

// publishing { publications { create<MavenPublication>("maven") { from(components["java"]) } } }
publishing {
  publications { create<MavenPublication>("maven") { from(components["java"]) } }
  repositories {
    maven {
      name = "GITHUB"
      val releasesRepoUrl =
        System.getenv("GITHUB_URL").takeUnless { it.isNullOrEmpty() }
          ?: extra["GITHUB_URL"].toString()
      url = uri(releasesRepoUrl)
      credentials {
        username =
          System.getenv("GITHUB_ACTOR").takeUnless { it.isNullOrEmpty() }
            ?: extra["GITHUB_ACTOR"].toString()
        password =
          System.getenv("GITHUB_TOKEN").takeUnless { it.isNullOrEmpty() }
            ?: extra["GITHUB_TOKEN"].toString()
      }
    }
    maven {
      name = "NEXUS"
      val nexusUrl =
        System.getenv("NEXUS_URL").takeUnless { it.isNullOrEmpty() }
          ?: extra["NEXUS_URL"].toString()
      val releasesRepoUrl = nexusUrl + "/maven-releases"
      val snapshotsRepoUrl = nexusUrl + "/maven-snapshots"
      url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
      isAllowInsecureProtocol =
        System.getenv().containsKey("NEXUS_INSECURE").takeUnless { it == false }
          ?: extra["NEXUS_INSECURE"].toString().toBoolean()
      credentials {
        username =
          System.getenv("NEXUS_USERNAME").takeUnless { it.isNullOrEmpty() }
            ?: extra["NEXUS_USERNAME"].toString()
        password =
          System.getenv("NEXUS_PASSWORD").takeUnless { it.isNullOrEmpty() }
            ?: extra["NEXUS_PASSWORD"].toString()
      }
    }
  }
}

repositories { mavenCentral() }

java { toolchain { languageVersion.set(JavaLanguageVersion.of(17)) } }

dependencies {
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.6.0")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
  implementation("org.slf4j:slf4j-jdk14:1.7.30")
  annotationProcessor("org.immutables:value:2.8.8")
  compileOnly("org.immutables:value-annotations:2.8.8")
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
}

val submodulesUpdate by
  tasks.creating(Exec::class) {
    group = "Build Setup"
    description = "Updates (and inits) substrait git submodule"
    commandLine = listOf("git", "submodule", "update", "--init", "--recursive")
  }

allprojects {
  repositories { mavenCentral() }

  tasks.configureEach<Test> {
    val javaToolchains = project.extensions.getByType<JavaToolchainService>()
    useJUnitPlatform()
    javaLauncher.set(javaToolchains.launcherFor { languageVersion.set(JavaLanguageVersion.of(11)) })
  }
  tasks.withType<JavaCompile> {
    sourceCompatibility = "17"
    if (project.name != "core") {
      options.release.set(11)
    } else {
      options.release.set(8)
    }
    dependsOn(submodulesUpdate)
  }

  group = "io.substrait"
  version = "1.0-SNAPSHOT"

  plugins.withType<SpotlessPlugin>().configureEach {
    configure<SpotlessExtension> {
      kotlinGradle { ktfmt().googleStyle() }
      java {
        googleJavaFormat()
        removeUnusedImports()
        trimTrailingWhitespace()
        targetExclude("**/build/**")
      }
    }
  }
}
