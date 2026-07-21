# Building plans

`SubstraitBuilder` (in `io.substrait.dsl`) is the recommended high-level way to
construct a Substrait `Plan`. It wraps the immutable POJO builders with concise,
type-aware helpers for relations (scan, filter, project, join, aggregate, ...),
expressions (literals, field references, casts, functions), and the plan root.

## Creating a builder

```java
import io.substrait.dsl.SubstraitBuilder;

--8<-- "core/src/test/java/io/substrait/docs/BuildingPlansDocTest.java:create-builder"
```

The no-arg constructor uses `DefaultExtensionCatalog.DEFAULT_COLLECTION`, which
loads the standard Substrait function extensions (arithmetic, comparison,
boolean, aggregate, string, datetime, and more). To resolve custom functions,
pass your own `SimpleExtension.ExtensionCollection` — see
[Function & type extensions](extensions.md).

```java
--8<-- "core/src/test/java/io/substrait/docs/BuildingPlansDocTest.java:create-builder-custom"
```

## Type shortcuts

Types are created with `TypeCreator`. Throughout these examples we use two
aliases, matching the convention used across the codebase and tests:

```java
import io.substrait.type.TypeCreator;

--8<-- "core/src/test/java/io/substrait/docs/BuildingPlansDocTest.java:type-shortcuts"
```

So `R.I32` is a non-nullable 32-bit integer and `N.STRING` is a nullable string.
See [Types](types.md) for the full catalog.

## Relation helpers take lambdas

Most relation helpers accept a function from the input relation to the
expression(s) they need. This lets the builder resolve field references against
the input's schema for you. For example, `filter` takes a
`Function<Rel, Expression>` that produces the condition:

```java
--8<-- "core/src/test/java/io/substrait/docs/BuildingPlansDocTest.java:filter-lambda"
```

`project`, `aggregate`, `sort`, and `join` follow the same pattern: the lambda
receives the input relation (or, for joins, a `JoinInput` exposing `left()` and
`right()`).

## End-to-end example

The following assembles a plan that scans a table, filters it, projects two
columns, and wraps the result in a plan root with output names:

```java
import io.substrait.dsl.SubstraitBuilder;
import io.substrait.plan.Plan;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.type.TypeCreator;
import java.util.List;

--8<-- "core/src/test/java/io/substrait/docs/BuildingPlansDocTest.java:end-to-end"
```

!!! note
    `root(input, names)` validates that the number of output names matches the
    field count of the input relation, so keep the names list in sync with the
    final projection.

`b.plan(root)` uses a default execution behavior of
`VariableEvaluationMode.PER_PLAN`. Overloads accept a custom
`Plan.ExecutionBehavior` and/or multiple roots.

## What the builder covers

- **Relations** — `namedScan`, `filter`, `project`, `innerJoin` / `join`,
  `aggregate`, `sort`, `fetch` / `limit` / `offset`, `cross`, `set`,
  `emptyVirtualTableScan`, `namedWrite`, `namedUpdate`, `expand`, and the
  physical joins `hashJoin` / `mergeJoin` / `nestedLoopJoin`. See
  [Relations](relations.md).
- **Expressions** — literal helpers (`bool`, `i8`..`i64`, `fp32`, `fp64`, `str`),
  `fieldReference`(s), `cast`, arithmetic (`add`, `subtract`, `multiply`,
  `divide`, `negate`), comparison/boolean (`equal`, `and`, `or`, `not`,
  `isNull`), `ifThen`, `switchExpression`, and the generic `scalarFn` /
  `aggregateFn` / `windowFn`. See [Expressions & literals](expressions.md).
- **Aggregates** — `grouping`, `measure`, and the shortcuts `count`,
  `countStar`, `sum`, `sum0`, `min`, `max`, `avg`.
- **Plan assembly** — `root`, `plan`, and `remap` for output field remapping.

Once you have a `Plan`, convert it to protobuf for storage or exchange as
described in [Serialization](serialization.md).
