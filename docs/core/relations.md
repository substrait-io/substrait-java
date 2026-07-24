# Relations

Relations are the operators that make up a plan's query tree. Each is an
immutable POJO under `io.substrait.relation` with a static `builder()`, and each
carries an input (or inputs), the operator-specific configuration, and an
optional output `Rel.Remap`.

You can build relations two ways:

- with the [`SubstraitBuilder`](building-plans.md) DSL, whose helpers resolve
  field references and function declarations for you; or
- directly with the POJO `builder()` when you need full control.

The examples below use the `R` / `N` type aliases from [Types](types.md) and a
`SubstraitBuilder b`.

## NamedScan

Reads a named table with a fixed schema.

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:named-scan-dsl"
```

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:named-scan-direct"
```

## Filter

Keeps rows for which the condition evaluates to true.

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:filter"
```

## Project

Computes a list of output expressions from the input.

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:project"
```

## Join

`Join` is the logical join. The DSL exposes `innerJoin` and the generic `join`
(with an explicit `Join.JoinType`); the condition lambda receives a `JoinInput`
exposing `left()` and `right()`.

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:join-dsl"
```

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:join-direct"
```

!!! tip
    The builder also offers the physical joins `hashJoin`, `mergeJoin`, and
    `nestedLoopJoin`. `hashJoin`/`mergeJoin` take parallel lists of left/right
    key indexes and build equality join keys for you.

## Aggregate

An `Aggregate` combines groupings with measures. `aggregate` takes a lambda for
the grouping(s) and one for the measures:

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:aggregate"
```

Measure shortcuts include `count`, `countStar`, `sum`, `sum0`, `min`, `max`, and
`avg`. For any other aggregate, build the invocation with `aggregateFn` and wrap
it in a measure:

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:aggregate-fn"
```

## Sort

Orders rows by one or more sort fields. `sortFields(rel, indexes...)` produces
ascending, nulls-last sort fields; `sortField(expr, direction)` gives full
control.

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:sort"
```

## Fetch (limit / offset)

`Fetch` skips and/or limits rows. The DSL exposes `limit`, `offset`, and the
combined `fetch`:

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:fetch"
```

## Cross

Cartesian product of two inputs.

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:cross"
```

## Set

Combines multiple inputs with a set operation. `Set.SetOp` includes
`UNION_ALL`, `UNION_DISTINCT`, `MINUS_PRIMARY`, `INTERSECTION_PRIMARY`, and more.

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:set"
```

## VirtualTableScan

An inline table of literal rows. Build it directly with a schema and one or more
row expressions:

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:virtual-table"
```

The DSL also offers `emptyVirtualTableScan()` for a schema-less, row-less table.

## NamedWrite

Writes an input relation to a named table. Specify the write operation, the
create mode, and the output mode:

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:named-write"
```

`NamedUpdate` (via `b.namedUpdate(...)`) is the analogous update operator, taking
transformation expressions and a row-selection condition.

## Output remapping

Every relation helper has an overload accepting a `Rel.Remap`, and the input
POJO builders accept `.remap(...)`. A remap reorders or filters the operator's
output columns by index:

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:output-remap"
```

## Building a plan

Wrap the top relation in a `Plan.Root` and a `Plan`:

```java
--8<-- "core/src/test/java/io/substrait/docs/RelationsDocTest.java:building-a-plan"
```

See [Building plans](building-plans.md) for the full end-to-end flow and
[Serialization](serialization.md) to convert the result to protobuf.
