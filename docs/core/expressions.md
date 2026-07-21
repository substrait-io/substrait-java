# Expressions & literals

Expressions are POJOs under `io.substrait.expression.Expression`. There are two
convenient ways to create them:

- `io.substrait.expression.ExpressionCreator` — static factory methods for
  literals and function invocations, where you control nullability explicitly.
- The [`SubstraitBuilder`](building-plans.md) instance helpers — terse,
  extension-aware shortcuts (non-nullable literals, field references, casts,
  arithmetic and boolean functions) that resolve function declarations for you.

## Literals with ExpressionCreator

`ExpressionCreator` factories take a leading `nullable` flag followed by the
value. This is the most direct way to create a literal of a specific
nullability:

```java
import io.substrait.expression.ExpressionCreator;

--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:literals"
```

There are factories for the full range of literal kinds, including
`i8`/`i16`/`i64`, `fp32`, `binary`, `fixedChar`/`varChar`, `decimal`
(from a `BigDecimal` or two's-complement `ByteString`), the temporal
`precisionTime`/`precisionTimestamp`/`precisionTimestampTZ`, interval literals,
`uuid`, and the nested `list`/`emptyList`, `map`/`emptyMap`, and
`struct`/`nestedStruct` builders.

A typed null literal carries the type it stands in for:

```java
--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:typed-null"
```

## Casts, function invocations, and control flow

`ExpressionCreator` also builds non-literal expressions:

```java
--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:creator-nonliteral"
```

It further provides `aggregateFunction`, `windowFunction`, `switchStatement` /
`switchClause`, `ifThenStatement` / `ifThenClause`, and the execution-context
variables `currentDate`, `currentTimezone`, `currentTimestamp`. The `declaration`
argument comes from an extension collection — see
[Function & type extensions](extensions.md).

## Builder expression helpers

When you already have a `SubstraitBuilder`, its instance methods are usually the
easiest path. Literal helpers produce **non-nullable** literals:

```java
--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:builder-literals"
```

### Field references

Reference an input column by zero-based index; the builder resolves the field
type from the relation's schema:

```java
--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:field-references"
```

For joins, `fieldReference(JoinInput, index)` indexes across the combined
left-then-right schema.

### Casts

```java
--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:builder-cast"
```

### Arithmetic, comparison, and boolean helpers

These resolve the right function variant from the default extension collection
based on the argument types, and compute a sensible output type (including
nullability):

```java
--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:arithmetic-boolean"
```

!!! note
    Nullability is propagated automatically: for example `add` yields a nullable
    result if either operand is nullable, and `equal` always returns a
    non-nullable boolean.

### Generic function invocations

For any function not covered by a named helper, call `scalarFn` (or `aggregateFn`
/ `windowFn`) with the extension URN, the function key, the output type, and the
arguments:

```java
--8<-- "core/src/test/java/io/substrait/docs/ExpressionsDocTest.java:generic-scalarfn"
```

Aggregate measures (`count`, `sum`, `min`, `max`, `avg`, and the generic
`aggregateFn`) are covered on the [Relations](relations.md) page, where they are
used inside an `Aggregate`.

## Next steps

- Plug these expressions into relations in [Relations](relations.md).
- Round-trip them through protobuf in [Serialization](serialization.md).
