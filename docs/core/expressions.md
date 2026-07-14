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

Expression.I32Literal a  = ExpressionCreator.i32(false, 76);        // non-nullable 76
Expression.StrLiteral  s  = ExpressionCreator.string(true, "hello"); // nullable "hello"
Expression.BoolLiteral b  = ExpressionCreator.bool(false, true);
Expression.FP64Literal f  = ExpressionCreator.fp64(false, 2.5);
Expression.DateLiteral d  = ExpressionCreator.date(false, 19_000);   // days since epoch
```

There are factories for the full range of literal kinds, including
`i8`/`i16`/`i64`, `fp32`, `binary`, `fixedChar`/`varChar`, `decimal`
(from a `BigDecimal` or two's-complement `ByteString`), the temporal
`precisionTime`/`precisionTimestamp`/`precisionTimestampTZ`, interval literals,
`uuid`, and the nested `list`/`emptyList`, `map`/`emptyMap`, and
`struct`/`nestedStruct` builders.

A typed null literal carries the type it stands in for:

```java
Expression.NullLiteral n = ExpressionCreator.typedNull(TypeCreator.NULLABLE.I32);
```

## Casts, function invocations, and control flow

`ExpressionCreator` also builds non-literal expressions:

```java
// cast with an explicit failure behavior
Expression cast =
    ExpressionCreator.cast(
        TypeCreator.REQUIRED.I64, someExpression, Expression.FailureBehavior.THROW_EXCEPTION);

// scalar function invocation from a resolved declaration
Expression.ScalarFunctionInvocation call =
    ExpressionCreator.scalarFunction(declaration, TypeCreator.REQUIRED.I32, argExpr1, argExpr2);
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
SubstraitBuilder b = new SubstraitBuilder();

b.bool(true);        // BoolLiteral
b.i8(10);            // I8Literal
b.i16(100);          // I16Literal
b.i32(1000);         // I32Literal
b.i64(10_000L);      // I64Literal
b.fp32(1.5f);        // FP32Literal
b.fp64(2.5);         // FP64Literal
b.str("foo");        // StrLiteral
```

### Field references

Reference an input column by zero-based index; the builder resolves the field
type from the relation's schema:

```java
NamedScan scan =
    b.namedScan(List.of("t"), List.of("a", "b"),
        List.of(TypeCreator.REQUIRED.I32, TypeCreator.REQUIRED.STRING));

FieldReference col0 = b.fieldReference(scan, 0);
List<FieldReference> cols = b.fieldReferences(scan, 0, 1);
```

For joins, `fieldReference(JoinInput, index)` indexes across the combined
left-then-right schema.

### Casts

```java
Expression cast = b.cast(b.i32(1), TypeCreator.REQUIRED.I64);
```

### Arithmetic, comparison, and boolean helpers

These resolve the right function variant from the default extension collection
based on the argument types, and compute a sensible output type (including
nullability):

```java
Expression left = b.i32(10);
Expression right = b.i32(20);

b.add(left, right);       // add:i32_i32     -> functions_arithmetic
b.subtract(left, right);
b.multiply(left, right);
b.divide(left, right);
b.negate(left);

b.equal(left, right);     // equal:any_any   -> functions_comparison, returns boolean
b.and(cond1, cond2);      // and:bool        -> functions_boolean
b.or(cond1, cond2);
b.not(cond);
b.isNull(expr);           // is_null:any     -> functions_comparison
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
Expression.ScalarFunctionInvocation substr =
    b.scalarFn(
        DefaultExtensionCatalog.FUNCTIONS_STRING,
        "substring:str_i32_i32",
        TypeCreator.REQUIRED.STRING,
        strArg, startArg, lengthArg);
```

Aggregate measures (`count`, `sum`, `min`, `max`, `avg`, and the generic
`aggregateFn`) are covered on the [Relations](relations.md) page, where they are
used inside an `Aggregate`.

## Next steps

- Plug these expressions into relations in [Relations](relations.md).
- Round-trip them through protobuf in [Serialization](serialization.md).
