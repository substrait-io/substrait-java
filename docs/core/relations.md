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
// DSL: names, column names, column types
NamedScan scan =
    b.namedScan(List.of("t"), List.of("a", "b"), List.of(R.I32, R.STRING));
```

```java
// Direct builder
NamedScan scan =
    NamedScan.builder()
        .addNames("test_table")
        .initialSchema(
            NamedStruct.builder()
                .addNames("only_column")
                .struct(R.struct(R.I32))
                .build())
        .build();
```

## Filter

Keeps rows for which the condition evaluates to true.

```java
Filter filter =
    b.filter(rel -> b.equal(b.fieldReference(rel, 0), b.i32(10)), scan);
```

## Project

Computes a list of output expressions from the input.

```java
Project project =
    b.project(
        rel -> List.of(b.i32(1), b.fieldReference(rel, 0)),
        scan);
```

## Join

`Join` is the logical join. The DSL exposes `innerJoin` and the generic `join`
(with an explicit `Join.JoinType`); the condition lambda receives a `JoinInput`
exposing `left()` and `right()`.

```java
Join join =
    b.innerJoin(
        inputs ->
            b.equal(
                b.fieldReference(inputs.left(), 0),
                b.fieldReference(inputs.right(), 0)),
        left,
        right);
```

```java
// Direct builder, choosing the join type explicitly
Join join =
    Join.builder()
        .left(leftTable)
        .right(rightTable)
        .condition(ExpressionCreator.bool(false, true))
        .joinType(Join.JoinType.LEFT)
        .build();
```

!!! tip
    The builder also offers the physical joins `hashJoin`, `mergeJoin`, and
    `nestedLoopJoin`. `hashJoin`/`mergeJoin` take parallel lists of left/right
    key indexes and build equality join keys for you.

## Aggregate

An `Aggregate` combines groupings with measures. `aggregate` takes a lambda for
the grouping(s) and one for the measures:

```java
Aggregate aggregate =
    b.aggregate(
        rel -> b.grouping(rel, 1),                    // GROUP BY column 1
        rel -> List.of(b.count(rel, 0), b.sum(b.fieldReference(rel, 0))),
        scan);
```

Measure shortcuts include `count`, `countStar`, `sum`, `sum0`, `min`, `max`, and
`avg`. For any other aggregate, build the invocation with `aggregateFn` and wrap
it in a measure:

```java
AggregateFunctionInvocation afi =
    b.aggregateFn(
        DefaultExtensionCatalog.FUNCTIONS_AGGREGATE_GENERIC,
        "count:any",
        R.I64,
        b.fieldReference(scan, 0));

Aggregate.Measure measure = b.measure(afi);
```

## Sort

Orders rows by one or more sort fields. `sortFields(rel, indexes...)` produces
ascending, nulls-last sort fields; `sortField(expr, direction)` gives full
control.

```java
Sort sort = b.sort(rel -> b.sortFields(rel, 0), scan);
```

## Fetch (limit / offset)

`Fetch` skips and/or limits rows. The DSL exposes `limit`, `offset`, and the
combined `fetch`:

```java
Fetch limited = b.limit(10, scan);          // first 10 rows
Fetch skipped = b.offset(5, scan);          // skip 5 rows
Fetch window  = b.fetch(0, 10, scan);       // offset 0, count 10
```

## Cross

Cartesian product of two inputs.

```java
Cross cross = b.cross(left, right);
```

## Set

Combines multiple inputs with a set operation. `Set.SetOp` includes
`UNION_ALL`, `UNION_DISTINCT`, `MINUS_PRIMARY`, `INTERSECTION_PRIMARY`, and more.

```java
Set union = b.set(Set.SetOp.UNION_ALL, input1, input2);
```

## VirtualTableScan

An inline table of literal rows. Build it directly with a schema and one or more
row expressions:

```java
VirtualTableScan table =
    VirtualTableScan.builder()
        .initialSchema(
            NamedStruct.of(List.of("col1"), R.struct(R.I32)))
        .addRows(ExpressionCreator.nestedStruct(false, ExpressionCreator.i32(false, 3)))
        .build();
```

The DSL also offers `emptyVirtualTableScan()` for a schema-less, row-less table.

## NamedWrite

Writes an input relation to a named table. Specify the write operation, the
create mode, and the output mode:

```java
NamedWrite write =
    b.namedWrite(
        List.of("target_table"),
        List.of("c1", "c2"),
        AbstractWriteRel.WriteOp.INSERT,
        AbstractWriteRel.CreateMode.APPEND_IF_EXISTS,
        AbstractWriteRel.OutputMode.MODIFIED_RECORDS,
        scan);
```

`NamedUpdate` (via `b.namedUpdate(...)`) is the analogous update operator, taking
transformation expressions and a row-selection condition.

## Output remapping

Every relation helper has an overload accepting a `Rel.Remap`, and the input
POJO builders accept `.remap(...)`. A remap reorders or filters the operator's
output columns by index:

```java
Rel.Remap remap = b.remap(0, 1);            // keep columns 0 and 1
Sort sort = b.sort(rel -> b.sortFields(rel, 0), remap, scan);
```

## Building a plan

Wrap the top relation in a `Plan.Root` and a `Plan`:

```java
Plan.Root root = b.root(project, List.of("a", "b"));
Plan plan = b.plan(root);
```

See [Building plans](building-plans.md) for the full end-to-end flow and
[Serialization](serialization.md) to convert the result to protobuf.
