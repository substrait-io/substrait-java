plugins {
  `kotlin-dsl`
  alias(libs.plugins.spotless)
}

repositories { gradlePluginPortal() }

// Pin the precompiled-script-plugin compilation to Java 17 so the generated plugin classes load on
// the daemon regardless of which JDK it runs on. Without this, kotlin-dsl targets whatever JDK the
// daemon happens to use (e.g. 21/25), emitting newer bytecode that a Java 17 daemon cannot load
// (UnsupportedClassVersionError).
kotlin { jvmToolchain(17) }

spotless { kotlinGradle { ktfmt().googleStyle() } }
