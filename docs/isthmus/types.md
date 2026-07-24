# Types & type system

Converting between SQL and Substrait means converting between Calcite's type model
(`RelDataType`) and Substrait's (`io.substrait.type.Type`). Isthmus does this with two
pieces:

- **`SubstraitTypeSystem`** — a Calcite `RelDataTypeSystem` whose precision and scale
  rules match Substrait's, plus the type factory built on top of it.
- **`TypeConverter`** — the bidirectional mapper between Calcite and Substrait types.

## `SubstraitTypeSystem`

`SubstraitTypeSystem` extends Calcite's default type system and adjusts the limits that
matter for Substrait:

| Setting | Value |
| --- | --- |
| Max `DECIMAL` precision | 38 |
| Max `DECIMAL` scale | 38 |
| Max precision for `TIME`, `TIMESTAMP`, `TIMESTAMP_WITH_LOCAL_TIME_ZONE`, and the year/day interval types | 6 (microseconds) |
| `shouldConvertRaggedUnionTypesToVarying()` | `true` |

Two shared singletons are exposed:

```java
public static final RelDataTypeSystem  TYPE_SYSTEM;   // the type system
public static final RelDataTypeFactory TYPE_FACTORY;  // a JavaTypeFactoryImpl over it
```

`ConverterProvider` uses `TYPE_FACTORY` by default, so all Isthmus conversions run with
this type system unless you override the type factory.

!!! note "Why the public no-arg constructor exists"
    Prefer the `TYPE_SYSTEM` singleton. The public no-argument constructor is kept
    because Calcite's `Frameworks`/Avatica machinery re-instantiates a type system from
    its class name (via a default constructor) when it is supplied to a
    `FrameworkConfig`. The type system is stateless, so any instance is equivalent to
    the singleton — do not remove the constructor.

## `TypeConverter`

`TypeConverter` maps types in both directions:

```java
// Calcite -> Substrait
Type toSubstrait(RelDataType type);
NamedStruct toNamedStruct(RelDataType rowType);

// Substrait -> Calcite
RelDataType toCalcite(RelDataTypeFactory factory, TypeExpression typeExpression);
```

`TypeConverter.DEFAULT` handles all the built-in types and does not map user-defined
types. To support user-defined types, construct one with a `UserTypeMapper`
(see [Customization](customization.md)).

### Type mapping

| Substrait | Calcite `SqlTypeName` |
| --- | --- |
| `bool` | `BOOLEAN` |
| `i8` | `TINYINT` |
| `i16` | `SMALLINT` |
| `i32` | `INTEGER` |
| `i64` | `BIGINT` |
| `fp32` | `REAL` |
| `fp64` | `DOUBLE` (`FLOAT` also maps to `fp64`) |
| `decimal(p, s)` | `DECIMAL(p, s)` |
| `string` | `VARCHAR` (unbounded) |
| `varchar(n)` | `VARCHAR(n)` |
| `fixedchar(n)` | `CHAR(n)` |
| `binary` | `VARBINARY` |
| `fixedbinary(n)` | `BINARY(n)` |
| `date` | `DATE` |
| `precision_time(p)` | `TIME(p)` |
| `precision_timestamp(p)` | `TIMESTAMP(p)` |
| `precision_timestamp_tz(p)` | `TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)` |
| `interval_year` | year-to-month interval |
| `interval_day` | day-to-second interval |
| `struct` | `ROW` |
| `list` | `ARRAY` |
| `map` | `MAP` |
| `user-defined` | via `UserTypeMapper` |

Nullability is preserved in both directions. A `DECIMAL` with precision greater than 38
is rejected with `UnsupportedOperationException`, and a `precision_time` /
`precision_timestamp` whose precision exceeds the type system's maximum (6) is rejected
with `IllegalArgumentException`.

## Decimal precision caveat (important)

When converting **Substrait to Calcite**, the Calcite `RelBuilder` must be created with
`SubstraitTypeSystem.TYPE_SYSTEM`.

Calcite's default type system caps `DECIMAL` precision at 19, while Substrait — and the
expressions Isthmus produces — carry decimals at precision up to 38. If a `RelBuilder`
built on the *default* type system processes a converted plan, the precision-38 types on
the expressions disagree with the type system's precision-19 ceiling. Calcite's
expression simplification (`RexSimplify`, run by `RelBuilder.project`) then re-derives
decimal arithmetic at precision 19 and wraps the result in a truncating
`CAST(... AS DECIMAL(19, 0))`, silently discarding scale and precision. Using the
Substrait type system keeps Calcite's type derivation aligned with the types the
expressions actually carry, so no truncating cast is inserted.

The default `ConverterProvider` already builds its `RelBuilder` with the Substrait type
system (its `getRelBuilder` supplies `getTypeSystem()`, which is derived from the
Substrait type factory). This caveat matters only if you build a `RelBuilder` yourself
for Substrait-to-Calcite conversion — always set its type system to
`SubstraitTypeSystem.TYPE_SYSTEM`.

## Related

- [Substrait to Calcite](substrait-to-calcite.md) — where the `RelBuilder` is used.
- [Customization](customization.md) — supplying a `UserTypeMapper` and a custom
  `TypeConverter`.
- [core types](../core/types.md) — the Substrait type model itself.
