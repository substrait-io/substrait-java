import java.nio.charset.StandardCharsets

plugins {
  id("idea")
  id("application")
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
  implementation(libs.classgraph)
  implementation(libs.guava)
  compileOnly(libs.graal.sdk)
  implementation(libs.picocli)
  annotationProcessor(libs.picocli.codegen)
  implementation(libs.protobuf.java.util) {
    exclude("com.google.guava", "guava")
      .because("Brings in Guava for Android, which we don't want (and breaks multimaps).")
  }
  annotationProcessor(libs.immutables.processor)
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

application { mainClass.set("io.substrait.isthmus.cli.IsthmusEntryPoint") }

graalvmNative {
  binaries {
    toolchainDetection.set(false)

    all { verbose.set(true) }

    named("main") {
      imageName.set("isthmus")
      fallback.set(false)
      buildArgs.add("-H:IncludeResources=.*yaml")
      buildArgs.add("--report-unsupported-elements-at-runtime")
      buildArgs.add("-H:+ReportExceptionStackTraces")
      buildArgs.add("-H:DynamicProxyConfigurationFiles=${project.file("proxies.json")}")
      buildArgs.add("--features=io.substrait.isthmus.cli.RegisterAtRuntime")
      buildArgs.add("--future-defaults=all")
      jvmArgs.add("--sun-misc-unsafe-memory-access=allow")

      // Capture the classpath since native-compile erases it
      val isthmusClasspath = classpath.joinToString(":")
      jvmArgs.add("-Disthmus.classpath=$isthmusClasspath")
    }
  }
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
