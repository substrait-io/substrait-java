plugins {
  `kotlin-dsl`
  id("com.diffplug.spotless") version "6.19.0"
}

repositories { gradlePluginPortal() }

spotless {
  kotlinGradle { ktfmt().googleStyle() }
}
