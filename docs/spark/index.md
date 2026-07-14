# Spark

The `spark` module bridges [Apache Spark](https://spark.apache.org/) and Substrait. It converts a
Spark **logical plan** into a Substrait [`Plan`](../core/building-plans.md) and, in the other
direction, rebuilds a Spark logical plan from a Substrait plan so that a Spark session can execute
it. This lets a query authored in one Spark cluster be serialized as an engine-neutral Substrait
plan, moved elsewhere, and run — or handed to a different engine entirely.

The module is written in Scala and published for several Spark/Scala combinations. Because the
public API is small and mostly plain method calls, it is comfortable to drive from Java as well;
the examples in these pages are Java calling the Scala API.

## The two entry points

Everything centres on two classes in the `io.substrait.spark.logical` package:

| Direction | Class | Key method |
| --- | --- | --- |
| Spark → Substrait | `ToSubstraitRel` | `convert(LogicalPlan): io.substrait.plan.Plan` |
| Substrait → Spark | `ToLogicalPlan` | `convert(io.substrait.plan.Plan): LogicalPlan` |

### `ToSubstraitRel` (Spark → Substrait)

`ToSubstraitRel` is a visitor over Spark's Catalyst `LogicalPlan` tree. Its `convert` method walks
the plan and produces an `io.substrait.plan.Plan` POJO, tagging the plan with the producer name
`substrait-spark`. From there you serialize to the canonical protobuf wire format with core's
`PlanProtoConverter`. See [Producing plans](producing-plans.md).

```java
ToSubstraitRel toSubstrait = new ToSubstraitRel();
io.substrait.plan.Plan plan = toSubstrait.convert(optimizedPlan);
```

### `ToLogicalPlan` (Substrait → Spark)

`ToLogicalPlan` is a Substrait `RelVisitor` that rebuilds a Catalyst `LogicalPlan`. It is
constructed with an active `SparkSession` (it needs the session to resolve `NamedScan` tables and
build file relations), and its `convert` method accepts either a Substrait `Plan` or a bare `Rel`.
See [Consuming plans](consuming-plans.md).

```java
ToLogicalPlan toSpark = new ToLogicalPlan(spark);
LogicalPlan sparkPlan = toSpark.convert(plan);
```

## Key convention: convert the optimized plan

!!! warning "Convert the optimized plan, not the raw logical plan"
    Always feed `ToSubstraitRel` the plan returned by
    `queryExecution().optimizedPlan()`, not `queryExecution().logical()`.

    Spark's raw logical plan still contains constructs that have no Substrait counterpart —
    `SubqueryAlias`, `View`, unresolved references, and Spark-internal rewrites. The Catalyst
    optimizer lowers these into the small set of relations and expressions the converter
    understands (projects, filters, joins, aggregates, and so on), so the optimized plan is the
    reliable starting point. This holds whether the query originated from SQL or the
    DataFrame/Dataset API — both produce the same optimized plan.

## Pages in this section

- [Compatibility & dependencies](compatibility.md) — the per-variant artifacts (there is no single
  cross-version jar) and how to depend on the one matching your Spark and Scala runtime.
- [Producing plans](producing-plans.md) — turn a Spark SQL or DataFrame query into a Substrait
  plan and serialize it.
- [Consuming plans](consuming-plans.md) — deserialize a Substrait plan and execute it on Spark.
- [Supported features](supported-features.md) — the types, expressions, relations, and functions
  the converters understand.
- [End-to-end example](end-to-end.md) — the runnable `substrait-spark` example: build, run a Spark
  cluster in Docker, produce a `.plan` file, then load and execute it.

!!! note "API reference"
    These pages cover the entry points and the common workflow. For the full API — every visitor,
    type mapping, and helper — see the published Javadoc for the variant you depend on, e.g.
    [`spark35_2.12`](https://javadoc.io/doc/io.substrait/spark35_2.12).
