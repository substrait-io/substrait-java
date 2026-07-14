# Getting started

This page shows how to add substrait-java to your build and produce your first Substrait plan.

## Requirements

- **Java 17 or newer** to run applications that depend on substrait-java.
- A build tool such as Gradle or Maven.

## Add a dependency

substrait-java is published to Maven Central under the group `io.substrait`. Add the module you
need. Most applications start with **core**; add **isthmus** if you want SQL conversion, or a
**spark** variant for Spark integration.

!!! tip "Check the latest version"
    The examples below use `0.95.1`. Check
    [Maven Central](https://central.sonatype.com/namespace/io.substrait) for the newest release
    and substitute it.

=== "Gradle (Kotlin DSL)"

    ```kotlin
    dependencies {
        implementation("io.substrait:core:0.95.1")
        // Optional: SQL <-> Substrait conversion
        implementation("io.substrait:isthmus:0.95.1")
    }
    ```

=== "Gradle (Groovy DSL)"

    ```groovy
    dependencies {
        implementation 'io.substrait:core:0.95.1'
        // Optional: SQL <-> Substrait conversion
        implementation 'io.substrait:isthmus:0.95.1'
    }
    ```

=== "Maven"

    ```xml
    <dependency>
      <groupId>io.substrait</groupId>
      <artifactId>core</artifactId>
      <version>0.95.1</version>
    </dependency>
    ```

The available artifacts are:

| Module | Maven artifact | Purpose |
| --- | --- | --- |
| Core | `io.substrait:core` | Plan model + protobuf conversion |
| Isthmus | `io.substrait:isthmus` | SQL ⇄ Substrait (Calcite) |
| Spark | `io.substrait:spark34_2.12`, `spark35_2.12`, `spark40_2.13` | Spark ⇄ Substrait (per Spark/Scala version — see [Spark compatibility](spark/compatibility.md)) |

!!! note "Logging"
    Core uses the [SLF4J](https://www.slf4j.org/) logging API. If you want log output, add an
    SLF4J provider to your runtime classpath; otherwise substrait-java runs fine but stays silent.

## Your first plan

The `SubstraitBuilder` DSL from the core module is the quickest way to assemble a plan. This
example builds a plan that scans an `orders` table and filters it, then serializes it to the
Substrait protobuf form.

```java
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.Rel;
import io.substrait.type.TypeCreator;
import java.util.List;

SubstraitBuilder builder = new SubstraitBuilder();
TypeCreator R = TypeCreator.REQUIRED;

// SELECT id, customer FROM orders WHERE id = 1
Rel scan =
    builder.namedScan(
        List.of("orders"),
        List.of("id", "customer"),
        List.of(R.I32, R.STRING));

Rel filtered =
    builder.filter(
        input -> builder.equal(builder.fieldReference(input, 0), builder.i32(1)),
        scan);

Plan.Root root = builder.root(filtered, List.of("id", "customer"));
Plan plan = builder.plan(root);

// Convert the POJO plan to the Substrait protobuf message
io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
byte[] bytes = proto.toByteArray();
```

That `byte[]` (or its JSON form) is a portable Substrait plan any Substrait-aware engine can
consume. See [Serialization](core/serialization.md) for the round trip back to a POJO and for the
JSON representation.

## Where to go next

- **[Core](core/index.md)** — build plans in Java, model types and expressions, and serialize.
- **[Isthmus](isthmus/index.md)** — convert SQL to and from Substrait.
- **[Isthmus CLI](isthmus-cli/index.md)** — do the same from the command line.
- **[Spark](spark/index.md)** — produce and consume plans with Apache Spark.
