plugins {
  `maven-publish`
  signing
  id("java-library")
  id("scala")
  id("idea")
  id("com.diffplug.spotless") version "7.1.0"
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

configurations.all {
  if (name.startsWith("incrementalScalaAnalysis")) {
    setExtendsFrom(emptyList())
  }
}

java {
  toolchain { languageVersion = JavaLanguageVersion.of(17) }
  withJavadocJar()
  withSourcesJar()
}

tasks.withType<ScalaCompile>() { scalaCompileOptions.additionalParameters = listOf("-release:17") }

var SLF4J_VERSION = properties.get("slf4j.version")
var SPARKBUNDLE_VERSION = properties.get("sparkbundle.version")
var SPARK_VERSION = properties.get("spark.version")

sourceSets {
  main { scala { setSrcDirs(listOf("src/main/scala", "src/main/spark-${SPARKBUNDLE_VERSION}")) } }
  test { scala { setSrcDirs(listOf("src/test/scala", "src/test/spark-3.2", "src/main/scala")) } }
}

dependencies {
  api(project(":core"))
  implementation("org.scala-lang:scala-library:2.12.16")
  api("org.apache.spark:spark-core_2.12:${SPARK_VERSION}")
  api("org.apache.spark:spark-sql_2.12:${SPARK_VERSION}")
  implementation("org.apache.spark:spark-catalyst_2.12:${SPARK_VERSION}")
  implementation("org.slf4j:slf4j-api:${SLF4J_VERSION}")

  testImplementation("org.scalatest:scalatest_2.12:3.2.18")
  testRuntimeOnly("org.junit.platform:junit-platform-engine:1.10.0")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.0")
  testRuntimeOnly("org.scalatestplus:junit-5-10_2.12:3.2.18.0")
  testImplementation("org.apache.spark:spark-core_2.12:${SPARK_VERSION}:tests")
  testImplementation("org.apache.spark:spark-sql_2.12:${SPARK_VERSION}:tests")
  testImplementation("org.apache.spark:spark-catalyst_2.12:${SPARK_VERSION}:tests")
}

spotless {
  scala {
    scalafmt().configFile(".scalafmt.conf")
    toggleOffOn()
  }
}

tasks {
  jar {
    manifest {
      from("../core/build/generated/sources/manifest/META-INF/MANIFEST.MF")
      attributes("Implementation-Title" to "substrait-spark")
    }
  }

  test {
    dependsOn(":core:shadowJar")
    useJUnitPlatform { includeEngines("scalatest") }
    jvmArgs("--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
  }
}
