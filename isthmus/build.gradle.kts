import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  `maven-publish`
  id("java")
  id("idea")
  id("com.palantir.graal") version "0.10.0"
  id("com.diffplug.spotless") version "6.11.0"
  id("com.github.johnrengelman.shadow") version "7.1.2"
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

java {
  toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
  withJavadocJar()
  withSourcesJar()
}

var CALCITE_VERSION = properties.get("calcite.version")
var IMMUTABLES_VERSION = properties.get("immutables.version")
var JACKSON_VERSION = properties.get("jackson.version")
var JUNIT_VERSION = properties.get("junit.version")

dependencies {
  implementation(project(":core"))
  implementation("org.apache.calcite:calcite-core:${CALCITE_VERSION}")
  implementation("org.apache.calcite:calcite-server:${CALCITE_VERSION}")
  implementation("org.junit.jupiter:junit-jupiter:${JUNIT_VERSION}")
  implementation("org.reflections:reflections:0.9.12")
  implementation("com.google.guava:guava:29.0-jre")
  implementation("org.graalvm.sdk:graal-sdk:22.1.0")
  implementation("info.picocli:picocli:4.7.5")
  annotationProcessor("info.picocli:picocli-codegen:4.7.5")
  implementation("com.fasterxml.jackson.core:jackson-databind:${JACKSON_VERSION}")
  implementation("com.fasterxml.jackson.core:jackson-annotations:${JACKSON_VERSION}")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8:${JACKSON_VERSION}")
  implementation("com.google.protobuf:protobuf-java-util:3.17.3") {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
  }
  implementation("com.google.code.findbugs:jsr305:3.0.2")
  implementation("com.github.ben-manes.caffeine:caffeine:3.0.4")
  implementation("org.immutables:value-annotations:${IMMUTABLES_VERSION}")
  annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")
  testImplementation("org.apache.calcite:calcite-plus:${CALCITE_VERSION}")
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
}

graal {
  mainClass("io.substrait.isthmus.IsthmusEntryPoint")
  outputName("isthmus")
  graalVersion("22.1.0")
  javaVersion("17")
  option("--no-fallback")
  option(
    "--initialize-at-build-time=io.substrait.isthmus.InitializeAtBuildTime,org.slf4j.impl.StaticLoggerBinder,com.google.common.math.IntMath\$1,com.google.common.base.Platform,com.google.common.util.concurrent.AbstractFuture\$UnsafeAtomicHelper,com.google.common.collect.ImmutableSortedMap,com.google.common.math.IntMath,com.google.common.collect.RegularImmutableSortedSet,com.google.common.cache.LocalCache,com.google.common.collect.Range,org.apache.commons.codec.language.Soundex,com.google.common.collect.ImmutableRangeSet,org.slf4j.LoggerFactory,com.google.common.collect.Platform,com.google.common.util.concurrent.SettableFuture,com.google.common.util.concurrent.AbstractFuture,com.google.common.util.concurrent.AbstractFuture,com.google.common.cache.CacheBuilder,com.google.common.base.Preconditions,com.google.common.collect.RegularImmutableMap,org.slf4j.impl.JDK14LoggerAdapter,org.apache.calcite.rel.metadata.RelMdColumnUniqueness,org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnOrigin,io.substrait.isthmus.metadata.LambdaMetadataSupplier,org.apache.calcite.rel.metadata.BuiltInMetadata\$PopulationSize,org.apache.calcite.rel.metadata.BuiltInMetadata\$Size,org.apache.calcite.rel.metadata.BuiltInMetadata\$UniqueKeys,org.apache.calcite.rel.metadata.RelMdColumnOrigins,org.apache.calcite.rel.metadata.RelMdExplainVisibility,org.apache.calcite.rel.metadata.RelMdMemory,org.apache.calcite.rel.metadata.RelMdExpressionLineage,org.apache.calcite.rel.metadata.RelMdDistinctRowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$RowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$PercentageOriginalRows,org.apache.calcite.util.Pair,org.apache.calcite.rel.metadata.BuiltInMetadata\$ExpressionLineage,org.apache.calcite.rel.metadata.BuiltInMetadata\$MinRowCount,com.google.common.primitives.Primitives,org.apache.calcite.rel.metadata.BuiltInMetadata\$Selectivity,org.apache.calcite.rel.metadata.BuiltInMetadata\$Parallelism,org.apache.calcite.rel.metadata.RelMdUniqueKeys,org.apache.calcite.rel.metadata.RelMdParallelism,org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows,org.apache.calcite.rel.metadata.BuiltInMetadata\$Predicates,org.apache.calcite.rel.metadata.BuiltInMetadata\$Distribution,org.apache.calcite.config.CalciteSystemProperty,org.apache.calcite.rel.metadata.BuiltInMetadata\$NonCumulativeCost,org.apache.calcite.util.Util,org.apache.calcite.rel.metadata.RelMdAllPredicates,io.substrait.isthmus.metadata.LambdaHandlerCache,org.apache.calcite.rel.metadata.BuiltInMetadata\$TableReferences,org.apache.calcite.rel.metadata.RelMdNodeTypes,org.apache.calcite.rel.metadata.RelMdCollation,org.apache.calcite.rel.metadata.RelMdSelectivity,org.apache.calcite.rel.metadata.BuiltInMetadata\$NodeTypes,org.apache.calcite.rel.metadata.RelMdPredicates,org.apache.calcite.rel.metadata.BuiltInMetadata\$DistinctRowCount,org.apache.calcite.rel.metadata.RelMdRowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$MaxRowCount,org.apache.calcite.rel.metadata.BuiltInMetadata\$AllPredicates,org.apache.calcite.rel.metadata.RelMdMaxRowCount,org.apache.calcite.rel.metadata.RelMdLowerBoundCost,org.apache.calcite.rel.metadata.BuiltInMetadata\$ExplainVisibility,org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnUniqueness,org.apache.calcite.rel.metadata.RelMdPopulationSize,org.apache.calcite.rel.metadata.BuiltInMetadata\$Memory,org.apache.calcite.rel.metadata.RelMdMinRowCount,org.apache.calcite.rel.metadata.RelMdSize,org.apache.calcite.rel.metadata.BuiltInMetadata\$LowerBoundCost,org.apache.calcite.rel.metadata.RelMdTableReferences,org.apache.calcite.rel.metadata.RelMdDistribution,io.substrait.isthmus.metadata.LegacyToLambdaGenerator,org.apache.calcite.rel.metadata.BuiltInMetadata\$CumulativeCost,org.apache.calcite.rel.metadata.BuiltInMetadata\$Collation"
  )
  option("-H:IncludeResources=.*yaml")
  option("--report-unsupported-elements-at-runtime")
  option("-H:+ReportExceptionStackTraces")
  option("-H:DynamicProxyConfigurationFiles=proxies.json")
  option("--features=io.substrait.isthmus.RegisterAtRuntime")
  option("-J--enable-preview")
}

tasks {
  named<ShadowJar>("shadowJar") {
    archiveBaseName.set("isthmus")
    manifest { attributes(mapOf("Main-Class" to "io.substrait.isthmus.PlanEntryPoint")) }
  }
}

tasks { build { dependsOn(shadowJar) } }
