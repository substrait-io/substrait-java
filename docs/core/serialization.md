# Serialization

The POJO model and the Substrait protobuf wire format are two representations of
the same plan. The `:core` module converts between them in both directions, and
the generated protobuf classes (package `io.substrait.proto`) handle the final
encoding to bytes or JSON.

## Plan: POJO to protobuf and back

The two entry points live in `io.substrait.plan`:

- `PlanProtoConverter.toProto(Plan)` — POJO `io.substrait.plan.Plan` to proto
  `io.substrait.proto.Plan`.
- `ProtoPlanConverter.from(io.substrait.proto.Plan)` — proto back to POJO.

```java
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;

--8<-- "core/src/test/java/io/substrait/docs/SerializationDocTest.java:plan-roundtrip"
```

Both converters default to `DefaultExtensionCatalog.DEFAULT_COLLECTION`. When
your plan references custom functions or types, pass a matching
`SimpleExtension.ExtensionCollection` (and, for advanced extensions, custom
extension converters) to the constructor:

```java
--8<-- "core/src/test/java/io/substrait/docs/SerializationDocTest.java:custom-collection"
```

See [Function & type extensions](extensions.md) for building extension
collections.

## Encoding to bytes

The proto `Plan` is a standard protobuf message, so serialize and parse it with
the usual protobuf API:

```java
--8<-- "core/src/test/java/io/substrait/docs/SerializationDocTest.java:encode-bytes"
```

This binary form is the canonical way to store or exchange plans between
Substrait producers and consumers.

## Encoding to JSON

For a human-readable form, use protobuf's `JsonFormat`:

```java
import com.google.protobuf.util.JsonFormat;

--8<-- "core/src/test/java/io/substrait/docs/SerializationDocTest.java:encode-json"
```

!!! note
    JSON is convenient for debugging, tests, and interop, but the binary form is
    more compact and is what most tooling exchanges.

## Lower-level converters

`PlanProtoConverter` and `ProtoPlanConverter` delegate to per-layer converters
that you can use directly when working with a single relation, expression, or
type. The naming tells you the direction: `<Thing>ProtoConverter` is POJO to
proto, `Proto<Thing>Converter` is proto to POJO.

| Layer | POJO to proto | proto to POJO |
| --- | --- | --- |
| Relations | `RelProtoConverter` | `ProtoRelConverter` |
| Expressions | `ExpressionProtoConverter` | `ProtoExpressionConverter` |
| Types | `TypeProtoConverter` | `ProtoTypeConverter` |

These converters thread an `ExtensionCollector` so that function and type
references discovered while walking the tree are gathered into the plan's
extension declarations. A minimal relation round-trip wires them together like
this:

```java
import io.substrait.extension.ExtensionCollector;
import io.substrait.relation.RelProtoConverter;
import io.substrait.relation.ProtoRelConverter;

--8<-- "core/src/test/java/io/substrait/docs/SerializationDocTest.java:lower-level"
```

!!! tip
    Test code in `:core` extends `io.substrait.TestBase` and calls
    `verifyRoundTrip(Rel)` / `verifyRoundTrip(Expression)` to assert
    POJO to proto to POJO fidelity — a useful pattern to mirror in your own tests.

## Related

- Build the plans you serialize in [Building plans](building-plans.md).
- Serialize schema-bound standalone expressions via
  [Extended expressions](extended-expressions.md).
