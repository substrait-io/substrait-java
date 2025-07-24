plugins {
  id("java")
  id("idea")
  id("com.palantir.graal") version "0.10.0"
  id("com.diffplug.spotless") version "7.1.0"
  id("substrait.java-conventions")
}

java {
  toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
  withJavadocJar()
  withSourcesJar()
}

configurations { runtimeClasspath { resolutionStrategy.activateDependencyLocking() } }

val CALCITE_VERSION = properties.get("calcite.version")
val GUAVA_VERSION = properties.get("guava.version")
val IMMUTABLES_VERSION = properties.get("immutables.version")
val JUNIT_VERSION = properties.get("junit.version")
val PROTOBUF_VERSION = properties.get("protobuf.version")
val SLF4J_VERSION = properties.get("slf4j.version")

dependencies {
  implementation(project(":core"))
  implementation(project(":isthmus"))
  testImplementation(platform("org.junit:junit-bom:${JUNIT_VERSION}"))
  testImplementation("org.junit.jupiter:junit-jupiter")
  testRuntimeOnly("org.junit.platform:junit-platform-launcher")
  implementation("org.reflections:reflections:0.9.12")
  implementation("com.google.guava:guava:${GUAVA_VERSION}")
  implementation("org.graalvm.sdk:graal-sdk:22.1.0")
  implementation("info.picocli:picocli:4.7.5")
  annotationProcessor("info.picocli:picocli-codegen:4.7.5")
  implementation("com.google.protobuf:protobuf-java-util:${PROTOBUF_VERSION}") {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
  }
  implementation("org.immutables:value-annotations:${IMMUTABLES_VERSION}")
  annotationProcessor("org.immutables:value:${IMMUTABLES_VERSION}")
  testImplementation("org.apache.calcite:calcite-plus:${CALCITE_VERSION}") {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  annotationProcessor("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  compileOnly("com.github.bsideup.jabel:jabel-javac-plugin:0.4.2")
  runtimeOnly("org.slf4j:slf4j-jdk14:${SLF4J_VERSION}")
}

val initializeAtBuildTime =
  listOf(
      "com.google.common.base.Platform",
      "com.google.common.base.Preconditions",
      "com.google.common.cache.CacheBuilder",
      "com.google.common.cache.LocalCache",
      "com.google.common.collect.CollectCollectors",
      "com.google.common.collect.ImmutableRangeSet",
      "com.google.common.collect.ImmutableSortedMap",
      "com.google.common.collect.Platform",
      "com.google.common.collect.Range",
      "com.google.common.collect.RegularImmutableMap",
      "com.google.common.collect.RegularImmutableSortedSet",
      "com.google.common.math.IntMath",
      "com.google.common.math.IntMath\$1",
      "com.google.common.primitives.Primitives",
      "com.google.common.util.concurrent.AbstractFuture",
      "com.google.common.util.concurrent.AbstractFuture\$UnsafeAtomicHelper",
      "com.google.common.util.concurrent.SettableFuture",
      "io.substrait.isthmus.cli.InitializeAtBuildTime",
      "io.substrait.isthmus.metadata.LambdaHandlerCache",
      "io.substrait.isthmus.metadata.LambdaMetadataSupplier",
      "io.substrait.isthmus.metadata.LegacyToLambdaGenerator",
      "org.apache.calcite.config.CalciteSystemProperty",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$AllPredicates",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$Collation",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnOrigin",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$ColumnUniqueness",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$CumulativeCost",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$DistinctRowCount",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$Distribution",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$ExplainVisibility",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$ExpressionLineage",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$LowerBoundCost",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$MaxRowCount",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$Memory",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$MinRowCount",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$NodeTypes",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$NonCumulativeCost",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$Parallelism",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$PercentageOriginalRows",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$PopulationSize",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$Predicates",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$RowCount",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$Selectivity",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$Size",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$TableReferences",
      "org.apache.calcite.rel.metadata.BuiltInMetadata\$UniqueKeys",
      "org.apache.calcite.rel.metadata.RelMdAllPredicates",
      "org.apache.calcite.rel.metadata.RelMdCollation",
      "org.apache.calcite.rel.metadata.RelMdColumnOrigins",
      "org.apache.calcite.rel.metadata.RelMdColumnUniqueness",
      "org.apache.calcite.rel.metadata.RelMdDistinctRowCount",
      "org.apache.calcite.rel.metadata.RelMdDistribution",
      "org.apache.calcite.rel.metadata.RelMdExplainVisibility",
      "org.apache.calcite.rel.metadata.RelMdExpressionLineage",
      "org.apache.calcite.rel.metadata.RelMdLowerBoundCost",
      "org.apache.calcite.rel.metadata.RelMdMaxRowCount",
      "org.apache.calcite.rel.metadata.RelMdMemory",
      "org.apache.calcite.rel.metadata.RelMdMinRowCount",
      "org.apache.calcite.rel.metadata.RelMdNodeTypes",
      "org.apache.calcite.rel.metadata.RelMdParallelism",
      "org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows",
      "org.apache.calcite.rel.metadata.RelMdPopulationSize",
      "org.apache.calcite.rel.metadata.RelMdPredicates",
      "org.apache.calcite.rel.metadata.RelMdRowCount",
      "org.apache.calcite.rel.metadata.RelMdSelectivity",
      "org.apache.calcite.rel.metadata.RelMdSize",
      "org.apache.calcite.rel.metadata.RelMdTableReferences",
      "org.apache.calcite.rel.metadata.RelMdUniqueKeys",
      "org.apache.calcite.util.Pair",
      "org.apache.calcite.util.ReflectUtil",
      "org.apache.calcite.util.Util",
      "org.apache.commons.codec.language.Soundex",
      "org.slf4j.LoggerFactory",
      "org.slf4j.impl.JDK14LoggerAdapter",
      "org.slf4j.impl.StaticLoggerBinder",
    )
    .joinToString(",")

graal {
  mainClass("io.substrait.isthmus.cli.IsthmusEntryPoint")
  outputName("isthmus")
  graalVersion("22.1.0")
  javaVersion("17")
  option("--no-fallback")
  option("--initialize-at-build-time=${initializeAtBuildTime}")
  option("-H:IncludeResources=.*yaml")
  option("--report-unsupported-elements-at-runtime")
  option("-H:+ReportExceptionStackTraces")
  option("-H:DynamicProxyConfigurationFiles=${project.file("proxies.json")}")
  option("--features=io.substrait.isthmus.cli.RegisterAtRuntime")
  option("-J--enable-preview")
}
