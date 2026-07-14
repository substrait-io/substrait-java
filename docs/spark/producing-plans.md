# Producing plans

Producing a Substrait plan from Spark is a three-step pipeline:

1. Get Spark to build the query's **optimized** logical plan.
2. Convert that plan to an `io.substrait.plan.Plan` with `ToSubstraitRel`.
3. Serialize the plan to protobuf bytes with core's `PlanProtoConverter`.

The query can start from either the SQL API or the DataFrame/Dataset API — both funnel into the
same optimized plan, so the conversion code is identical from step 2 onward.

## Step 1: get the optimized logical plan

!!! warning "Use `optimizedPlan()`"
    `ToSubstraitRel` expects the **optimized** logical plan
    (`queryExecution().optimizedPlan()`). The raw logical plan still contains `SubqueryAlias`,
    `View`, and unresolved nodes that the converter cannot translate; the optimizer rewrites these
    into the relations and expressions Substrait understands. See the
    [overview](index.md#key-convention-convert-the-optimized-plan).

=== "SQL API"

    ```java
    // A DataFrame from a SQL string; tables/views must already be registered
    Dataset<Row> result = spark.sql(
        "SELECT vehicles.colour, count(*) AS colourcount"
            + " FROM vehicles"
            + " INNER JOIN tests ON vehicles.vehicle_id = tests.vehicle_id"
            + " WHERE tests.test_result = 'P'"
            + " GROUP BY vehicles.colour"
            + " ORDER BY count(*)");

    LogicalPlan optimised = result.queryExecution().optimizedPlan();
    ```

=== "DataFrame / Dataset API"

    ```java
    Dataset<Row> joined =
        dsVehicles
            .join(dsTests, dsVehicles.col("vehicle_id").equalTo(dsTests.col("vehicle_id")))
            .filter(dsTests.col("test_result").equalTo("P"))
            .groupBy(dsVehicles.col("colour"))
            .count()
            .orderBy("count");

    LogicalPlan optimised = joined.queryExecution().optimizedPlan();
    ```

Structurally the two optimized plans are identical, so the Substrait plan produced from each is the
same.

## Step 2: convert to a Substrait `Plan`

`ToSubstraitRel.convert` walks the Catalyst plan and returns an `io.substrait.plan.Plan` POJO. The
plan is stamped with the producer name `substrait-spark`.

```java
import io.substrait.spark.logical.ToSubstraitRel;

ToSubstraitRel toSubstrait = new ToSubstraitRel();
io.substrait.plan.Plan plan = toSubstrait.convert(optimised);
```

`io.substrait.plan.Plan` is a high-level, immutable POJO. You can inspect or transform it in memory,
but most often you will serialize it.

!!! tip "Truncating in-memory (RDD) sources"
    When the plan reads from an in-memory `LogicalRDD` (for example a DataFrame created from a local
    collection), `ToSubstraitRel` captures the rows as a Substrait `VirtualTableScan`. To keep plans
    bounded it takes at most `rddLimit` rows (default `100`) and logs a warning if there are more.
    Adjust it before converting:

    ```java
    ToSubstraitRel toSubstrait = new ToSubstraitRel();
    toSubstrait.rddLimit_$eq(1000); // Scala setter, seen from Java
    ```

## Step 3: serialize to protobuf

The canonical Substrait serialization is protobuf. Core's `PlanProtoConverter` turns the POJO plan
into a protobuf `io.substrait.proto.Plan`, from which you get the wire bytes:

```java
import io.substrait.plan.PlanProtoConverter;

byte[] buffer = new PlanProtoConverter().toProto(plan).toByteArray();

// e.g. persist the plan to a file
Files.write(Paths.get("spark_substrait.plan"), buffer);
```

Those bytes are the portable intermediate representation: store them, ship them to another engine,
or reload them into Spark. See [Serialization](../core/serialization.md) for the full round-trip
details and [Consuming plans](consuming-plans.md) for the reverse direction.

!!! note "Shortcut: `toProtoSubstrait`"
    `ToSubstraitRel` also exposes `toProtoSubstrait(LogicalPlan): byte[]`, which performs the
    convert-and-serialize in one call. It emits a bare relation tree (via `RelProtoConverter`)
    rather than a full `Plan` with root output names, so `convert` followed by `PlanProtoConverter`
    is preferred when you need the complete plan — for example to round-trip through
    [`ToLogicalPlan`](consuming-plans.md).

## Not everything converts

The converter supports the common relations, expressions, and functions — enough that every TPC-H
query round-trips — but it is not exhaustive. Unsupported nodes raise
`UnsupportedOperationException` (for example union-by-name, or a file format other than
CSV/Parquet/ORC). See [Supported features](supported-features.md) for the full list.
