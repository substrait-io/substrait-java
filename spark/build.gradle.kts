// Orchestrator project for building all Spark variants
// The actual variant builds are in subprojects: spark-3.4_2.12, spark-3.5_2.12, spark-4.0_2.13

plugins {
  id("base") // Provides lifecycle tasks like clean, build, assemble
}

// Aggregate task to build all variants
tasks.register("buildAllVariants") {
  group = "build"
  description = "Builds all Spark/Scala variants"
  dependsOn(
    ":spark:spark-3.4_2.12:build",
    ":spark:spark-3.5_2.12:build",
    ":spark:spark-4.0_2.13:build"
  )
}

// Aggregate task to publish all variants to Maven Local
tasks.register("publishAllVariants") {
  group = "publishing"
  description = "Publishes all Spark/Scala variants to Maven Local"
  dependsOn(
    ":spark:spark-3.4_2.12:publishToMavenLocal",
    ":spark:spark-3.5_2.12:publishToMavenLocal",
    ":spark:spark-4.0_2.13:publishToMavenLocal"
  )
}

// Make the default build task build all variants
tasks.named("build") {
  dependsOn("buildAllVariants")
}

// Make clean task clean all variants
tasks.named("clean") {
  dependsOn(
    ":spark:spark-3.4_2.12:clean",
    ":spark:spark-3.5_2.12:clean",
    ":spark:spark-4.0_2.13:clean"
  )
}
