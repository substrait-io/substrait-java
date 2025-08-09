plugins {
  `kotlin-dsl`
  alias(libs.plugins.spotless)
}

repositories { gradlePluginPortal() }

spotless { kotlinGradle { ktfmt().googleStyle() } }
