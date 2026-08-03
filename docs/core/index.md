# Core

The `:core` module is the heart of substrait-java. It provides an immutable POJO
model for Substrait plans, relations, expressions, and types, together with
bidirectional conversion to and from the Substrait protobuf wire format and the
machinery for handling function and type extensions.

Everything the other modules build on lives here: [Isthmus](../isthmus/index.md)
(Calcite SQL conversion) and the Spark integration both produce and consume the
POJO model described on these pages.

## Add the dependency

```xml
<dependency>
  <groupId>io.substrait</groupId>
  <artifactId>core</artifactId>
  <version>0.95.1</version>
</dependency>
```

!!! tip
    `0.95.1` is the version documented here. Check
    [the javadoc index](https://javadoc.io/doc/io.substrait/core) for the latest
    released version and pin accordingly.

## The POJO model

The model is a tree of immutable value objects generated with
[Immutables](https://immutables.github.io/). For every abstract type such as
`Expression`, `Type`, or `io.substrait.plan.Plan`, the build generates an
`Immutable<Type>` implementation, and the POJO exposes a static `builder()` that
delegates to it:

```java
import io.substrait.plan.Plan;
import io.substrait.relation.NamedScan;

--8<-- "core/src/test/java/io/substrait/docs/CoreIndexDocTest.java:quick-example"
```

Because the objects are immutable, they are safe to share and compare by value.
Builders support `addX`/`addAllX` for collection fields and `withX` copy methods
on the built instances.

!!! note
    Generated `Immutable*` classes only exist after the module is compiled, so an
    IDE may report `ImmutableExpression.*` and new `builder()` methods as
    unresolved until `:core:compileJava` has run once.

For most use cases you do not build these objects by hand. The
[`SubstraitBuilder` DSL](building-plans.md) wraps the builders with concise,
type-aware helpers and is the recommended entry point.

## The visitor pattern

Traversal of the model uses double-dispatch visitors. Each layer has a visitor
interface plus, in most cases, an abstract base with sensible defaults:

- **Expressions** — `ExpressionVisitor` (implement every case) and
  `AbstractExpressionVisitor` (override only what you need).
- **Relations** — `RelVisitor` / `AbstractRelVisitor`, plus copy-on-write
  transformers `RelCopyOnWriteVisitor` and `ExpressionCopyOnWriteVisitor`.
- **Types** — `TypeVisitor`, extended by `ParameterizedTypeVisitor` and
  `TypeExpressionVisitor` for function-signature and derived-type expressions.

The proto converters are themselves visitors: `ExpressionProtoConverter`,
`RelProtoConverter`, and `TypeProtoConverter` walk the POJO model to produce
proto, while the `Proto<Thing>Converter` classes switch over the proto `oneof`
cases to rebuild POJOs. See [Serialization](serialization.md) for details.

## Map of the core documentation

| Page | What it covers |
| --- | --- |
| [Building plans](building-plans.md) | The `SubstraitBuilder` DSL — the recommended high-level way to assemble a `Plan`. |
| [Types](types.md) | `TypeCreator`: nullability, scalar constants, and parameterized types (decimal, struct, list, map). |
| [Expressions & literals](expressions.md) | `ExpressionCreator` factories and the builder's expression helpers (field references, casts, arithmetic and boolean functions). |
| [Relations](relations.md) | The relation operators (scan, filter, project, join, aggregate, sort, fetch, set, write) and how to build them. |
| [Serialization](serialization.md) | POJO to protobuf and back, plus binary and JSON encodings. |
| [Extended expressions](extended-expressions.md) | Standalone expressions evaluated against a schema, outside a full plan. |
| [Function & type extensions](extensions.md) | The default extension catalog, loading custom YAML, and advanced extensions. |

## API reference

Full Javadoc for every public class is published at
[javadoc.io/doc/io.substrait/core](https://javadoc.io/doc/io.substrait/core).
