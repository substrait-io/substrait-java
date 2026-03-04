# Multi-Variant Spark/Scala Build System

This document describes how to build and publish multiple Spark/Scala variants of the substrait-spark module.

## Supported Variants

The substrait-spark module supports three build variants:

| Variant | Spark Version | Scala Version | Classifier | Subproject |
|---------|---------------|---------------|------------|------------|
| Spark 3.4 | 3.4.4 | 2.12.20 | `spark34_2.12` | `:spark:spark-3.4_2.12` |
| Spark 3.5 | 3.5.4 | 2.12.20 | `spark35_2.12` | `:spark:spark-3.5_2.12` |
| Spark 4.0 | 4.0.2 | 2.13.18 | `spark40_2.13` | `:spark:spark-4.0_2.13` |

## Architecture

The build system uses **Gradle subprojects** for each variant.

### Project Structure

```
spark/
├── build.gradle.kts              # Orchestrator project
├── src/                          # Shared source code
│   ├── main/
│   │   ├── scala/                # Common code for all versions
│   │   ├── spark-3.4/            # Spark 3.4 specific implementations
│   │   ├── spark-3.5/            # Spark 3.5 specific implementations
│   │   └── spark-4.0/            # Spark 4.0 specific implementations
│   └── test/
│       ├── scala/                # Common test code
│       ├── spark-3.4/            # Spark 3.4 specific tests
│       ├── spark-3.5/            # Spark 3.5 specific tests
│       └── spark-4.0/            # Spark 4.0 specific tests
├── spark-3.4_2.12/
│   └── build.gradle.kts          # Spark 3.4 variant build
├── spark-3.5_2.12/
│   └── build.gradle.kts          # Spark 3.5 variant build
└── spark-4.0_2.13/
    └── build.gradle.kts          # Spark 4.0 variant build
```

Each subproject references the shared source code in `../src/` using Gradle's source set configuration.

## Building Variants

### Build a Specific Variant

Build a specific variant using its subproject path:

```bash
# Build Spark 3.4 with Scala 2.12
./gradlew :spark:spark-3.4_2.12:build

# Build Spark 3.5 with Scala 2.12
./gradlew :spark:spark-3.5_2.12:build

# Build Spark 4.0 with Scala 2.13
./gradlew :spark:spark-4.0_2.13:build
```

### Build All Variants

To build all variants:

```bash
./gradlew :spark:build
```

## Publishing Variants

### Publish to Local Maven Repository

Publish a specific variant:

```bash
# Publish Spark 3.4 with Scala 2.12
./gradlew :spark:spark-3.4_2.12:publishToMavenLocal

# Publish Spark 3.5 with Scala 2.12
./gradlew :spark:spark-3.5_2.12:publishToMavenLocal

# Publish Spark 4.0 with Scala 2.13
./gradlew :spark:spark-4.0_2.13:publishToMavenLocal
```

### Publish All Variants

To publish all variants to your local Maven repository:

```bash
./gradlew :spark:publishAllVariants
```

Published artifacts will be available at:
```
~/.m2/repository/io/substrait/{classifier}/{version}/
```

For example:
- `~/.m2/repository/io/substrait/spark34_2.12/0.78.0/`
- `~/.m2/repository/io/substrait/spark35_2.12/0.78.0/`
- `~/.m2/repository/io/substrait/spark40_2.13/0.78.0/`

### Publish to Maven Central Portal

Publish all variants to Maven Central:

```bash
./gradlew :spark:publishAllVariantsToCentralPortal
```

Or publish a specific variant:

```bash
./gradlew :spark:spark-4.0_2.13:publishMaven-publishPublicationToNmcpRepository
```

## Using Published Artifacts

### Maven

Add the appropriate variant as a dependency in your `pom.xml`:

```xml
<!-- Spark 3.4 with Scala 2.12 -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>spark34_2.12</artifactId>
    <version>0.80.0</version>
</dependency>

<!-- Spark 3.5 with Scala 2.12 -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>spark35_2.12</artifactId>
    <version>0.80.0</version>
</dependency>

<!-- Spark 4.0 with Scala 2.13 -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>spark40_2.13</artifactId>
    <version>0.80.0</version>
</dependency>
```

### Gradle

Add the appropriate variant as a dependency in your `build.gradle.kts`:

```kotlin
dependencies {
    // Spark 3.4 with Scala 2.12
    implementation("io.substrait:spark34_2.12:0.80.0")

    // Spark 3.5 with Scala 2.12
    implementation("io.substrait:spark35_2.12:0.80.0")

    // Spark 4.0 with Scala 2.13
    implementation("io.substrait:spark40_2.13:0.80.0")
}
```

## Development Workflow

### Adding Support for a New Spark Version

1. **Create a new subproject directory**:
   ```bash
   mkdir -p spark/spark-4.1_2.13
   ```

2. **Copy and modify a build.gradle.kts** from an existing variant:
   ```bash
   cp spark/spark-4.0_2.13/build.gradle.kts spark/spark-4.1_2.13/
   ```

3. **Update the variant configuration** in the new `build.gradle.kts`:
   ```kotlin
   val sparkVersion = "4.1.0"
   val scalaVersion = "2.13.18"
   val sparkMajorMinor = "4.1"
   val scalaBinary = "2.13"
   val classifier = "spark41_2.13"
   ```

4. **Add the subproject** to `settings.gradle.kts`:
   ```kotlin
   include(
     // ... existing projects
     "spark:spark-4.1_2.13",
   )
   ```

5. **Update the orchestrator** in `spark/build.gradle.kts`:
   ```kotlin
   tasks.register("buildAllVariants") {
     dependsOn(
       // ... existing variants
       ":spark:spark-4.1_2.13:build"
     )
   }
   ```

6. **Create version-specific source directory**:
   ```bash
   mkdir -p spark/src/main/spark-4.1
   mkdir -p spark/src/test/spark-4.1
   ```

7. **Add version-specific implementations** for classes with API differences

8. **Test the new variant**:
   ```bash
   ./gradlew :spark:spark-4.1_2.13:build
   ```

### Testing Changes Across All Variants

When making changes to common code, test all variants:

```bash
# Quick compilation test for all variants
./gradlew :spark:spark-3.4_2.12:compileScala
./gradlew :spark:spark-3.5_2.12:compileScala
./gradlew :spark:spark-4.0_2.13:compileScala

# Or run full build for all variants
./gradlew :spark:buildAllVariants
```

### Cleaning Build Artifacts

```bash
# Clean a specific variant
./gradlew :spark:spark-4.0_2.13:clean

# Clean all variants
./gradlew :spark:clean
```
