import java.nio.charset.StandardCharsets

plugins {
  id("java")
  id("idea")
  alias(libs.plugins.graal)
  alias(libs.plugins.spotless)
  id("substrait.java-conventions")
}

java {
  toolchain { languageVersion.set(JavaLanguageVersion.of(17)) }
  withJavadocJar()
  withSourcesJar()
}

configurations { runtimeClasspath { resolutionStrategy.activateDependencyLocking() } }

dependencies {
  implementation(project(":core"))
  implementation(project(":isthmus"))
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.junit.jupiter)
  testRuntimeOnly(libs.junit.platform.launcher)
  implementation(libs.reflections)
  implementation(libs.guava)
  implementation(libs.graal.sdk)
  implementation(libs.picocli)
  annotationProcessor(libs.picocli.codegen)
  implementation(libs.protobuf.java.util) {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
  }
  annotationProcessor(libs.immutables.value)
  implementation(libs.immutables.annotations)
  testImplementation(libs.calcite.plus) {
    exclude(group = "commons-lang", module = "commons-lang")
      .because(
        "calcite-core brings in commons-lang:commons-lang:2.4 which has a security vulnerability"
      )
  }
  runtimeOnly(libs.slf4j.jdk14)
}

sourceSets { main { java.srcDir("build/generated/sources/version/") } }

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

tasks.register("writeIsthmusVersion") {
  val version = project.version
  doLast {
    val isthmusVersionClass =
      layout.buildDirectory
        .file("generated/sources/version/io/substrait/isthmus/cli/IsthmusCliVersion.java")
        .get()
        .getAsFile()
    isthmusVersionClass.getParentFile().mkdirs()

    isthmusVersionClass.printWriter(StandardCharsets.UTF_8).use {
      it.println("package io.substrait.isthmus.cli;\n")
      it.println("import io.substrait.SubstraitVersion;")
      it.println("import picocli.CommandLine.IVersionProvider;\n")
      it.println("public class IsthmusCliVersion implements IVersionProvider {")
      it.println("  public String[] getVersion() throws Exception {")
      it.println("    return new String[] {")
      it.println("        \"\${COMMAND-NAME} version " + version + "\",")
      it.println("        \"Substrait version \" + SubstraitVersion.VERSION,")
      it.println("};")
      it.println("  }")
      it.println("}")
    }
  }
}

tasks.named("compileJava") { dependsOn("writeIsthmusVersion") }
