# Function & type extensions

Substrait keeps its function and type definitions out of the core specification
and in **extensions** — YAML documents that declare each function's name,
argument types, and return type. substrait-java loads these into a
`SimpleExtension.ExtensionCollection`, which the builder and converters use to
resolve a reference into a full declaration.

## The default catalog

`io.substrait.extension.DefaultExtensionCatalog` bundles the standard Substrait
extensions. `DEFAULT_COLLECTION` is a ready-to-use `ExtensionCollection`
containing all of them, and this is what a no-arg
[`SubstraitBuilder`](building-plans.md), `PlanProtoConverter`, and
`ProtoPlanConverter` use by default.

```java
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;

--8<-- "core/src/test/java/io/substrait/docs/ExtensionsDocTest.java:defaults"
```

The class also exposes the extension **URN** constants you pass when invoking a
function. URNs follow the format `extension:<namespace>:<name>` — for example
`FUNCTIONS_ARITHMETIC` is `extension:io.substrait:functions_arithmetic`. Among
them:

| Constant | Covers |
| --- | --- |
| `FUNCTIONS_ARITHMETIC` | add, subtract, multiply, divide, sum, min, max, avg, ... |
| `FUNCTIONS_COMPARISON` | equal, not_equal, is_null, lt, gt, ... |
| `FUNCTIONS_BOOLEAN` | and, or, not, ... |
| `FUNCTIONS_STRING` | substring, concat, ... |
| `FUNCTIONS_DATETIME` | date/time functions |
| `FUNCTIONS_AGGREGATE_GENERIC` | count, ... |
| `FUNCTIONS_AGGREGATE_APPROX` | approximate aggregates |
| `FUNCTIONS_ARITHMETIC_DECIMAL` | decimal arithmetic |

(`FUNCTIONS_ROUNDING`, `FUNCTIONS_LOGARITHMIC`, `FUNCTIONS_SET`,
`FUNCTIONS_LIST`, `FUNCTIONS_GEOMETRY`, and `EXTENSION_TYPES` are also declared.)

## Referencing a built-in function

Functions are addressed by a `FunctionAnchor` — a URN plus a **key** of the form
`name:arg_types` (for example `add:i32_i32` or `count:any`). The
[`SubstraitBuilder`](building-plans.md) resolves these for you. Named helpers
cover the common cases:

```java
--8<-- "core/src/test/java/io/substrait/docs/ExtensionsDocTest.java:named-helpers"
```

For anything else, call the generic `scalarFn` / `aggregateFn` with the URN, the
function key, the output type, and the arguments:

```java
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.type.TypeCreator;

--8<-- "core/src/test/java/io/substrait/docs/ExtensionsDocTest.java:generic-fns"
```

Under the hood these call `extensions.getScalarFunction(anchor)` /
`getAggregateFunction(anchor)` on the collection. Looking up a function whose URN
is not loaded throws `IllegalArgumentException`, so make sure the builder's
collection contains the extension you reference.

## Loading custom extensions

`SimpleExtension.load(...)` reads extension YAML into an `ExtensionCollection`.
There are three overloads:

```java
// from classpath resource paths
SimpleExtension.ExtensionCollection fromResources =
    SimpleExtension.load(List.of("/my/extensions/functions_custom.yaml"));

// from a YAML string
SimpleExtension.ExtensionCollection fromString = SimpleExtension.load(yamlContent);

// from an InputStream
SimpleExtension.ExtensionCollection fromStream = SimpleExtension.load(inputStream);
```

Each YAML document must declare a `urn` of the form `extension:<namespace>:<name>`;
loading validates it. A minimal custom extension declaring a type looks like:

```yaml
---
urn: extension:my.org:my_types
types:
  - name: point
    structure:
      x: i32
      y: i32
```

Combine your extensions with the defaults using `merge`, then hand the result to
a builder or converter:

```java
--8<-- "core/src/test/java/io/substrait/docs/ExtensionsDocTest.java:merge"
```

!!! warning
    The collection used when converting a plan must be able to resolve every
    function and type the plan references. Pass the same (merged) collection to
    both the builder and the `PlanProtoConverter` / `ProtoPlanConverter` so that
    round-tripping succeeds. See [Serialization](serialization.md).

## Advanced extensions

`io.substrait.extension.AdvancedExtension` carries producer-specific data
attached to a plan or relation. It has two parts:

- **optimizations** — optional hints that do not change semantics and may be
  ignored by a consumer; and
- an **enhancement** — a semantic change that a consumer must honor.

```java
import io.substrait.extension.AdvancedExtension;

--8<-- "core/src/test/java/io/substrait/docs/ExtensionsDocTest.java:advanced-extension"
```

Because the payloads are opaque to core, serializing or deserializing a plan that
carries them requires custom extension converters passed to `PlanProtoConverter`
/ `ProtoPlanConverter`; without them, conversion throws
`UnsupportedOperationException`.

## Related

- Invoke functions inside relations in [Relations](relations.md) and expressions
  in [Expressions & literals](expressions.md).
- Browse the full extension API in the
  [API reference](https://javadoc.io/doc/io.substrait/core).
