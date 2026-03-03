# Multi-Variant Spark/Scala Build System

This document describes how to build and publish multiple Spark/Scala variants of the substrait-spark module.

## Supported Variants

The substrait-spark module supports three build variants:

| Variant | Spark Version | Scala Version | Classifier |
|---------|---------------|---------------|------------|
| Spark 3.4 | 3.4.4 | 2.12.20 | `spark34_2.12` |
| Spark 3.5 | 3.5.4 | 2.12.20 | `spark35_2.12` |
| Spark 4.0 | 4.0.2 | 2.13.18 | `spark40_2.13` |

## Building Specific Variants

### List Available Variants

To see all available variants and their versions:

```bash
./gradlew :spark:listVariants
```

### Build a Specific Variant

To build a specific variant, use the `-PsparkVariant` property:

```bash
# Build Spark 3.4 with Scala 2.12
./gradlew :spark:build -PsparkVariant=spark34_2.12

# Build Spark 3.5 with Scala 2.12
./gradlew :spark:build -PsparkVariant=spark35_2.12

# Build Spark 4.0 with Scala 2.13 (default)
./gradlew :spark:build -PsparkVariant=spark40_2.13
# or simply
./gradlew :spark:build
```

### Build All Variants

To build all variants in sequence:

```bash
./gradlew :spark:buildAllVariants
```

This will:
1. Clean the build directory
2. Build each variant sequentially
3. Report success/failure for each variant

## Publishing Variants

### Publish to Local Maven Repository

To publish a specific variant to your local Maven repository (`~/.m2/repository`):

```bash
./gradlew :spark:publishToMavenLocal -PsparkVariant=spark34_2.12
```

### Publish All Variants

To publish all variants to your local Maven repository:

```bash
./gradlew :spark:publishAllVariants
```

Published artifacts will be available at:
```
~/.m2/repository/io/substrait/substrait-spark/{version}/
```

## Using Published Artifacts

### Maven

Add the appropriate variant as a dependency in your `pom.xml`:

```xml
<!-- Spark 3.4 with Scala 2.12 -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>substrait-spark</artifactId>
    <version>0.78.0</version>
    <classifier>spark34_2.12</classifier>
</dependency>

<!-- Spark 3.5 with Scala 2.12 -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>substrait-spark</artifactId>
    <version>0.78.0</version>
    <classifier>spark35_2.12</classifier>
</dependency>

<!-- Spark 4.0 with Scala 2.13 -->
<dependency>
    <groupId>io.substrait</groupId>
    <artifactId>substrait-spark</artifactId>
    <version>0.78.0</version>
    <classifier>spark40_2.13</classifier>
</dependency>
```

### Gradle

Add the appropriate variant as a dependency in your `build.gradle.kts`:

```kotlin
dependencies {
    // Spark 3.4 with Scala 2.12
    implementation("io.substrait:substrait-spark:0.78.0:spark34_2.12")

    // Spark 3.5 with Scala 2.12
    implementation("io.substrait:substrait-spark:0.78.0:spark35_2.12")

    // Spark 4.0 with Scala 2.13
    implementation("io.substrait:substrait-spark:0.78.0:spark40_2.13")
}
```

## Source Code Organization

The project uses version-specific source directories to handle API differences between Spark versions:

```
spark/src/
├── main/
│   ├── scala/              # Common code for all versions
│   ├── spark-3.4/          # Spark 3.4 specific implementations
│   ├── spark-3.5/          # Spark 3.5 specific implementations
│   └── spark-4.0/          # Spark 4.0 specific implementations
└── test/
    ├── scala/              # Common test code
    ├── spark-3.2/          # Spark 3.2 specific tests (legacy)
    ├── spark-4.0/          # Spark 4.0 specific tests
    └── ...
```

When building a specific variant, the build system automatically includes:
- Common source code from `src/main/scala`
- Version-specific code from `src/main/spark-{version}`

## Development Workflow

### Adding Support for a New Spark Version

1. **Add version definitions** to `gradle/libs.versions.toml`:
   ```toml
   [versions]
   spark-4-1 = "4.1.0"

   [libraries]
   spark-core-4-1-2-13 = { module = "org.apache.spark:spark-core_2.13", version.ref = "spark-4-1" }
   # ... add other Spark libraries
   ```

2. **Update variant list** in `spark/build.gradle.kts`:
   ```kotlin
   val sparkVariants = listOf(
       // ... existing variants
       SparkVariant("4.1.0", "2.13.18", "4.1", "2.13", "spark41_2.13")
   )
   ```

3. **Create version-specific source directory**:
   ```bash
   mkdir -p spark/src/main/spark-4.1
   ```

4. **Add version-specific implementations** for classes with API differences

5. **Test the new variant**:
   ```bash
   ./gradlew :spark:build -PsparkVariant=spark41_2.13
   ```

### Testing Changes Across All Variants

When making changes to common code, test all variants:

```bash
# Quick compilation test for all variants
./gradlew :spark:compileScala -PsparkVariant=spark34_2.12
./gradlew :spark:compileScala -PsparkVariant=spark35_2.12
./gradlew :spark:compileScala -PsparkVariant=spark40_2.13

# Or run full build for all variants
./gradlew :spark:buildAllVariants
```
