# Substrait to Calcite

`SubstraitToCalcite` converts Substrait relations into Apache Calcite relational trees.
It is the step that [Substrait to SQL](substrait-to-sql.md) builds on, but it is also
useful on its own when you want to hand a Substrait plan to Calcite for optimization,
execution, or further inspection as `RelNode`s.

## The `convert` methods

```java
public RelNode  convert(Rel rel);
public RelRoot  convert(Plan.Root root);
```

- `convert(Rel)` returns a Calcite `RelNode` — the relational operator tree for a
  single Substrait relation.
- `convert(Plan.Root)` returns a Calcite `RelRoot`, applying the root's final field
  names to the output row type (including nested struct, array, and map fields) and
  deriving the appropriate `SqlKind` (for example `INSERT`/`UPDATE`/`DELETE` for a
  table modification, otherwise the query kind).

## Constructing the converter

```java
public SubstraitToCalcite(ConverterProvider converterProvider);
public SubstraitToCalcite(ConverterProvider converterProvider, Prepare.CatalogReader catalogReader);
```

Calcite needs a schema to resolve the tables a plan reads from. There are two
strategies:

- **Supply a catalog** — pass a `Prepare.CatalogReader` (for example one built with
  `SubstraitCreateStatementParser.processCreateStatementsToCatalog(...)`, as in
  [SQL to Substrait](sql-to-substrait.md)). The converter resolves table names against
  it.
- **Let Isthmus infer the schema** — with the single-argument constructor, the
  `ConverterProvider`'s schema resolver walks the leaf (read) nodes of the plan and
  synthesizes a Calcite schema on the fly. Override
  `ConverterProvider#getSchemaResolver()` to customize this behavior.

## Example

Build a Substrait `Plan.Root` and convert it to a Calcite `RelRoot`:

```java
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.plan.Plan.Root;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.rel.RelRoot;

--8<-- "isthmus/src/test/java/io/substrait/isthmus/docs/SubstraitToCalciteDocTest.java:example"
```

(`sb` here is a `io.substrait.dsl.SubstraitBuilder`; see
[building plans](../core/building-plans.md).)

## Building the RelBuilder with the Substrait type system

Internally, `convert` obtains a Calcite `RelBuilder` from the `ConverterProvider`, and
that builder is created with the **Substrait type system**
(`SubstraitTypeSystem.TYPE_SYSTEM`, via the provider's type factory). This is essential
for correctness, not a detail: Calcite's default type system caps `DECIMAL` precision at
19, while Substrait carries decimals at precision up to 38. If the `RelBuilder` used a
mismatched type system, Calcite's expression simplification would re-derive decimal
arithmetic at the lower precision and wrap results in a truncating
`CAST(... AS DECIMAL(19, 0))`.

The default `ConverterProvider` already does the right thing. If you construct your own
`RelBuilder` for Substrait-to-Calcite work, be sure to set its type system to
`SubstraitTypeSystem.TYPE_SYSTEM`. See [Types & type system](types.md) for the full
explanation.

## Related

- [Substrait to SQL](substrait-to-sql.md) — renders the Calcite tree back to SQL text.
- [Types & type system](types.md) — the type mapping and the decimal-precision caveat.
- [Customization](customization.md) — customizing schema resolution and converters.
