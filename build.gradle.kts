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

publishing { publications { create<MavenPublication>("maven") { from(components["java"]) } } }

allprojects {
    publishing {
        publications {
            create<MavenPublication>("mavendavisusanibar") {
                from(components["java"])

                pom {
                    name.set("Java Gradle Semantic Release")
                    description.set("Implement semantic release for Java projects")
                    url.set("https://github.com/davisusanibar/poc-semantic-release-use-cases")
                    properties.set(mapOf(
                        "country" to "PE",
                        "dsusanibar.type.of" to "Java"
                    ))
                    licenses {
                        license {
                            name.set("The Apache License, Version 2.0")
                            url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                        }
                    }
                    developers {
                        developer {
                            id.set("davisusanibar")
                            name.set("david dali susanibar arce")
                            email.set("dsusanibara@uni.pe")
                        }
                    }
                    scm {
                        connection.set("scm:git:git://github.com:davisusanibar/poc-semantic-release-use-cases.git")
                        developerConnection.set("scm:git:ssh://github.com:davisusanibar/poc-semantic-release-use-cases")
                        url.set("https://github.com/davisusanibar/poc-semantic-release-use-cases/")
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
//            maven {
//                name = "github"
//                url = uri("https://github.com/davisusanibar/poc-semantic-release-use-cases")
//                credentials {
//                    username = System.getenv("GITHUB_ACTOR").takeUnless { it.isNullOrEmpty() } ?: extra["GITHUB_ACTOR"].toString()
//                    password = System.getenv("GITHUB_TOKEN").takeUnless { it.isNullOrEmpty() } ?: extra["GITHUB_TOKEN"].toString()
//                }
//            }
            maven {
                name = "sonatypeLocal"
                val sonatypeUser = System.getenv("sonatype_user").takeUnless { it.isNullOrEmpty() } ?: extra["sonatype_user"].toString()
                val sonatypePassword = System.getenv("sonatype_password").takeUnless { it.isNullOrEmpty() } ?: extra["sonatype_password"].toString()
                val releasesRepoUrl = "http://localhost:8081/repository/maven-releases/"
                val snapshotsRepoUrl = "http://localhost:8081/repository/maven-snapshots/"
                isAllowInsecureProtocol = true
                url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
                credentials {
                    username = sonatypeUser
                    password = sonatypePassword
                }
            }
        }
    }
//    nexusPublishing {
//        repositories {
//            create("sonatype") {
//                val sonatypeUser = System.getenv("sonatype_user").takeUnless { it.isNullOrEmpty() } ?: extra["sonatype_user"].toString()
//                val sonatypePassword = System.getenv("sonatype_password").takeUnless { it.isNullOrEmpty() } ?: extra["sonatype_password"].toString()
//                nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
//                snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
//                username.set(sonatypeUser)
//                password.set(sonatypePassword)
//            }
//        }
//    }
    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
        withJavadocJar()
        withSourcesJar()
    }
//    signing {
//        sign(publishing.publications["mavendavisusanibar"])
//    }
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
  version = "${version}"

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
