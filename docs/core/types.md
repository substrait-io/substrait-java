# Types

Substrait types are POJOs under `io.substrait.type.Type`. Rather than building
them one by one, you create them through `TypeCreator`, a factory that fixes the
nullability up front and exposes constants for the scalar types plus builders for
the parameterized ones.

## Nullability: REQUIRED vs NULLABLE

Every Substrait type is either nullable or not. `TypeCreator` has two shared
instances, one for each:

```java
import io.substrait.type.TypeCreator;

TypeCreator.REQUIRED; // produces non-nullable types
TypeCreator.NULLABLE; // produces nullable types
```

A common idiom (used throughout the codebase and the rest of these docs) is to
alias them as `R` and `N`:

```java
TypeCreator R = TypeCreator.REQUIRED;
TypeCreator N = TypeCreator.NULLABLE;
```

You can also select one dynamically with `TypeCreator.of(boolean)`:

```java
TypeCreator t = TypeCreator.of(nullable); // NULLABLE if true, else REQUIRED
```

To flip the nullability of an existing type, use the static helpers:

```java
io.substrait.type.Type nullableI32   = TypeCreator.asNullable(R.I32);
io.substrait.type.Type requiredI32   = TypeCreator.asNotNullable(N.I32);
```

## Scalar type constants

Each `TypeCreator` instance exposes constants for the simple types, already at
its nullability:

| Constant | Substrait type |
| --- | --- |
| `BOOLEAN` | boolean |
| `I8`, `I16`, `I32`, `I64` | 8/16/32/64-bit integers |
| `FP32`, `FP64` | single/double precision float |
| `STRING` | UTF-8 string |
| `BINARY` | variable-length binary |
| `DATE` | date |
| `INTERVAL_YEAR` | year-month interval |
| `UUID` | UUID |

```java
TypeCreator R = TypeCreator.REQUIRED;

io.substrait.type.Type i32    = R.I32;      // non-nullable i32
io.substrait.type.Type str    = R.STRING;   // non-nullable string
io.substrait.type.Type fp64   = R.FP64;     // non-nullable fp64
io.substrait.type.Type date   = R.DATE;     // non-nullable date
```

## Parameterized types

Parameterized types are created with instance methods that carry the creator's
nullability:

```java
TypeCreator R = TypeCreator.REQUIRED;

// decimal(precision, scale)
io.substrait.type.Type dec = R.decimal(10, 2);

// fixed- and variable-length character/binary
io.substrait.type.Type fchar  = R.fixedChar(20);
io.substrait.type.Type vchar  = R.varChar(255);
io.substrait.type.Type fbin   = R.fixedBinary(16);

// temporal types with fractional-second precision
io.substrait.type.Type ts     = R.precisionTimestamp(6);
io.substrait.type.Type tstz   = R.precisionTimestampTZ(6);
io.substrait.type.Type time   = R.precisionTime(6);
io.substrait.type.Type iday   = R.intervalDay(6);
```

### Structs, lists, and maps

```java
TypeCreator R = TypeCreator.REQUIRED;

// struct with i32 and string fields
io.substrait.type.Type.Struct struct = R.struct(R.I32, R.STRING);

// list of strings
io.substrait.type.Type list = R.list(R.STRING);

// map from string to i32
io.substrait.type.Type map = R.map(R.STRING, R.I32);
```

`struct(...)` also accepts an `Iterable<Type>` or a `Stream<Type>`, which is
handy when building a schema from a computed set of field types.

### User-defined types

Types declared by an extension are referenced by URN and name:

```java
io.substrait.type.Type udt =
    TypeCreator.REQUIRED.userDefined("extension:my.org:my_types", "point");
```

See [Function & type extensions](extensions.md) for how extension URNs work.

## Named structs

A relation's schema pairs field names with a struct type via
`io.substrait.type.NamedStruct`:

```java
import io.substrait.type.NamedStruct;

NamedStruct schema =
    NamedStruct.of(List.of("a", "b"), TypeCreator.REQUIRED.struct(R.I32, R.STRING));
```

The [`SubstraitBuilder`](building-plans.md) `namedScan` helper builds the
`NamedStruct` for you from parallel lists of column names and types.

## Next steps

- Use these types when creating literals and casts in
  [Expressions & literals](expressions.md).
- See the full type hierarchy in the
  [API reference](https://javadoc.io/doc/io.substrait/core).
