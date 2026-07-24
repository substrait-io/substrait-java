# Compatibility & dependencies

The `spark` module is published as **several independent artifacts**, one per supported
Spark/Scala combination. There is deliberately **no single cross-version jar**: each artifact is
compiled against a specific Spark release on a specific Scala binary version, and mixing them with
a different runtime will fail at load time. Pick the artifact that matches the Spark and Scala
versions your application already runs on.

## Variant matrix

| Variant | Spark | Scala | Maven artifact (`groupId:artifactId`) | Gradle subproject |
| --- | --- | --- | --- | --- |
| Spark 3.4 | 3.4.4 | 2.12 | `io.substrait:spark34_2.12` | `:spark:spark-3.4_2.12` |
| Spark 3.5 | 3.5.4 | 2.12 | `io.substrait:spark35_2.12` | `:spark:spark-3.5_2.12` |
| Spark 4.0 | 4.0.2 | 2.13 | `io.substrait:spark40_2.13` | `:spark:spark-4.0_2.13` |

The artifact id encodes both the Spark major/minor version and the Scala binary version
(`spark<major><minor>_<scalaBinary>`), matching the usual Scala convention. The **Gradle
subproject** column is only relevant when building from source in this repository; the shared Scala
source lives in `spark/src` and each subproject compiles it against its own Spark/Scala versions.

## Adding the dependency

The current release is **0.95.1** (group `io.substrait`). Add the single variant matching your
runtime.

### Spark 3.5 (Scala 2.12)

=== "Gradle"

    ```kotlin
    dependencies {
        implementation("io.substrait:spark35_2.12:0.95.1")
    }
    ```

=== "Maven"

    ```xml
    <dependency>
        <groupId>io.substrait</groupId>
        <artifactId>spark35_2.12</artifactId>
        <version>0.95.1</version>
    </dependency>
    ```

### Spark 3.4 (Scala 2.12)

=== "Gradle"

    ```kotlin
    dependencies {
        implementation("io.substrait:spark34_2.12:0.95.1")
    }
    ```

=== "Maven"

    ```xml
    <dependency>
        <groupId>io.substrait</groupId>
        <artifactId>spark34_2.12</artifactId>
        <version>0.95.1</version>
    </dependency>
    ```

### Spark 4.0 (Scala 2.13)

=== "Gradle"

    ```kotlin
    dependencies {
        implementation("io.substrait:spark40_2.13:0.95.1")
    }
    ```

=== "Maven"

    ```xml
    <dependency>
        <groupId>io.substrait</groupId>
        <artifactId>spark40_2.13</artifactId>
        <version>0.95.1</version>
    </dependency>
    ```

!!! tip "Match Scala binary versions"
    On Spark 3.4/3.5 use the Scala **2.12** variant; on Spark 4.0 use the Scala **2.13** variant.
    The Scala binary version must line up with the rest of your Spark application's dependencies —
    a 2.12 artifact will not load in a 2.13 runtime, and vice versa.

## Spark 4.0 package caveat

Spark 4.0 split the "classic" (`RDD`/Catalyst-backed) `SparkSession` and `Dataset` out into a new
package. The types you import differ by Spark version:

| Spark version | `SparkSession` / `Dataset` package |
| --- | --- |
| 3.4, 3.5 | `org.apache.spark.sql` |
| 4.0 | `org.apache.spark.sql.classic` |

!!! warning "Import the right `Dataset` / `SparkSession` on Spark 4.0"
    On Spark 4.0, the classic API used by these workflows — including the
    `Dataset.ofRows(spark, plan)` call that executes a converted plan (see
    [Consuming plans](consuming-plans.md)) — lives in
    `org.apache.spark.sql.classic.{Dataset, SparkSession}`.

    On Spark 3.4/3.5 the same classes are in `org.apache.spark.sql.{Dataset, SparkSession}`. Code
    that compiles against one variant will need its imports adjusted for the other. This is one of
    the reasons the artifacts are version-specific rather than shared.

## API reference

The published Javadoc is per variant. For example:

- [`spark34_2.12`](https://javadoc.io/doc/io.substrait/spark34_2.12)
- [`spark35_2.12`](https://javadoc.io/doc/io.substrait/spark35_2.12)
- [`spark40_2.13`](https://javadoc.io/doc/io.substrait/spark40_2.13)
