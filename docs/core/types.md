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
--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:aliases"
```

You can also select one dynamically with `TypeCreator.of(boolean)`:

```java
--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:of"
```

To flip the nullability of an existing type, use the static helpers:

```java
--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:flip-nullability"
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
--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:scalar-constants"
```

## Parameterized types

Parameterized types are created with instance methods that carry the creator's
nullability:

```java
--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:parameterized"
```

### Structs, lists, and maps

```java
--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:struct-list-map"
```

`struct(...)` also accepts an `Iterable<Type>` or a `Stream<Type>`, which is
handy when building a schema from a computed set of field types.

### User-defined types

Types declared by an extension are referenced by URN and name:

```java
--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:user-defined"
```

See [Function & type extensions](extensions.md) for how extension URNs work.

## Named structs

A relation's schema pairs field names with a struct type via
`io.substrait.type.NamedStruct`:

```java
import io.substrait.type.NamedStruct;

--8<-- "core/src/test/java/io/substrait/docs/TypesDocTest.java:named-struct"
```

The [`SubstraitBuilder`](building-plans.md) `namedScan` helper builds the
`NamedStruct` for you from parallel lists of column names and types.

## Next steps

- Use these types when creating literals and casts in
  [Expressions & literals](expressions.md).
- See the full type hierarchy in the
  [API reference](https://javadoc.io/doc/io.substrait/core).
