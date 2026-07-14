# Consuming plans

Consuming reverses [producing](producing-plans.md): starting from serialized Substrait bytes, you
rebuild a Spark logical plan and execute it. The pipeline is:

1. Parse the protobuf bytes into a protobuf `io.substrait.proto.Plan`.
2. Convert that to an `io.substrait.plan.Plan` POJO with core's `ProtoPlanConverter` — passing the
   **Spark extension collection**.
3. Rebuild a Spark `LogicalPlan` with `ToLogicalPlan`.
4. Execute it with `Dataset.ofRows`.

## Step 1: parse the protobuf bytes

```java
byte[] buffer = Files.readAllBytes(Paths.get("spark_substrait.plan"));
io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(buffer);
```

!!! tip "Name your variables for the two `Plan` types"
    There are two distinct `Plan` classes in play: the protobuf message
    `io.substrait.proto.Plan` and the high-level POJO `io.substrait.plan.Plan`. Using fully
    qualified names (or clearly named variables) keeps the two directions of conversion readable.

## Step 2: convert to a Substrait `Plan` POJO

`ProtoPlanConverter` turns the protobuf message back into the `io.substrait.plan.Plan` POJO. When
the plan was produced by Spark, deserialize it with `SparkExtension.COLLECTION`:

```java
import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.spark.SparkExtension;

Plan plan = new ProtoPlanConverter(SparkExtension.COLLECTION()).from(proto);
```

!!! warning "Pass `SparkExtension.COLLECTION` for Spark-produced plans"
    `SparkExtension.COLLECTION` is the standard Substrait function extensions **merged with the
    Spark-specific extensions** the producer may have used (for example `date_add`, or the
    aggregate/window functions declared in the Spark dialect). The default no-argument
    `ProtoPlanConverter()` only knows the standard extensions, so it will fail to resolve any
    Spark-specific function reference in the plan. Constructing the converter with
    `SparkExtension.COLLECTION()` ensures every function the plan references can be looked up.

    `SparkExtension` is a Scala `object`; from Java its `COLLECTION` value is reached as the static
    method call `SparkExtension.COLLECTION()`.

## Step 3: rebuild the Spark logical plan

`ToLogicalPlan` is constructed with the target `SparkSession` and converts the `Plan` POJO into a
Catalyst `LogicalPlan`. It needs the live session to resolve `NamedScan` table references against
the catalog and to build file-backed relations.

```java
import io.substrait.spark.logical.ToLogicalPlan;

ToLogicalPlan toSpark = new ToLogicalPlan(spark);
LogicalPlan sparkPlan = toSpark.convert(plan);
```

`convert(Plan)` also reapplies the plan's root output names, adding a final projection (with casts
if needed) so the executed plan's schema matches the names carried in the Substrait root. The
resulting plan is fully resolved and ready to run. (`ToLogicalPlan` also has a
`convert(io.substrait.relation.Rel)` overload for a bare relation tree without root names.)

## Step 4: execute

Hand the rebuilt logical plan to Spark for execution with `Dataset.ofRows`:

```java
Dataset.ofRows(spark, sparkPlan).show();
```

!!! warning "Spark 4.0 package"
    On Spark 4.0, `Dataset` (and the `SparkSession` you pass) come from
    `org.apache.spark.sql.classic`, whereas on Spark 3.4/3.5 they come from `org.apache.spark.sql`.
    See the [Spark 4.0 package caveat](compatibility.md#spark-40-package-caveat).

## Where the data comes from

What the plan reads from depends on which read relation the producer emitted:

- **`LocalFiles`** — the plan carries concrete file URIs (e.g.
  `file:///opt/spark-data/tests.csv`) plus format and read options. The consuming engine reads
  those files directly, so the paths must be valid where the plan runs.
- **`NamedScan`** — the plan carries a table name such as `[spark_catalog, default, vehicles]` and
  the expected schema, but no data location. The referenced table must already exist in the
  consuming session's catalog, otherwise execution fails.

This distinction matters most when moving plans between engines; see the
[end-to-end example](end-to-end.md#cross-engine-consumption) for how other engines handle each
case.
