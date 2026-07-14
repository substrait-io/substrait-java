# Isthmus

Isthmus is the SQL bridge in substrait-java. It converts SQL queries and SQL
expressions into [Substrait](https://substrait.io/) plans and expressions, and
converts Substrait back into SQL, using [Apache Calcite](https://calcite.apache.org/)
as the SQL parser, validator, and relational-algebra engine.

Under the hood every conversion path routes through Calcite's relational model:

```text
SQL  <->  Calcite RelNode / RexNode  <->  Substrait POJO  <->  Substrait protobuf
```

Isthmus wires up the Calcite parser, validator, type system, and operator table so
that the relational algebra it produces maps cleanly onto Substrait, and it supplies
the visitors that translate between Calcite's `RelNode`/`RexNode` trees and the
substrait-java [POJO model](../core/index.md).

## Add the dependency

```groovy
dependencies {
    implementation "io.substrait:isthmus:0.95.1"
}
```

!!! tip "Check the latest version"
    `0.95.1` is current at the time of writing. Check
    [Maven Central](https://central.sonatype.com/artifact/io.substrait/isthmus) for
    newer releases. The group is `io.substrait`.

The same functionality is available from the command line without writing any Java;
see the [Isthmus CLI](../isthmus-cli/index.md).

## The entry-point converters

Each direction of conversion has a small, focused entry-point class. All of them are
configured by a single `ConverterProvider`, whose no-argument constructor supplies
sensible system defaults (the standard Substrait extension catalog and the Substrait
type system):

```java
import io.substrait.isthmus.SqlToSubstrait;

// Uses ConverterProvider defaults
SqlToSubstrait converter = new SqlToSubstrait();
```

| Class | Direction | Returns |
| --- | --- | --- |
| `SqlToSubstrait` | SQL query -> Substrait | POJO `io.substrait.plan.Plan` |
| `SqlExpressionToSubstrait` | SQL expression -> Substrait | proto `io.substrait.proto.ExtendedExpression` |
| `SubstraitToSql` | Substrait -> SQL | one SQL `String` per plan root |
| `SubstraitToCalcite` | Substrait -> Calcite | `RelNode` / `RelRoot` |

`ConverterProvider` is the single point of configuration shared by all four. Pass a
customized provider (or one of its subclasses, such as `DynamicConverterProvider`) to
change extensions, functions, the type factory, or the SQL parser configuration. See
[Customization](customization.md).

## Pages in this section

- [SQL to Substrait](sql-to-substrait.md) — convert one or more SQL statements into a
  Substrait `Plan`, using a catalog built from `CREATE TABLE` statements.
- [SQL expressions](sql-expressions.md) — convert standalone SQL expressions into a
  Substrait `ExtendedExpression`.
- [Substrait to SQL](substrait-to-sql.md) — render a Substrait `Plan` back to SQL in a
  chosen dialect.
- [Substrait to Calcite](substrait-to-calcite.md) — convert Substrait relations into
  Calcite `RelNode`/`RelRoot` trees.
- [Types & type system](types.md) — how Calcite and Substrait types map, and the
  Substrait type system Isthmus relies on.
- [Customization](customization.md) — `ConverterProvider`, dynamic providers, custom
  functions/UDFs, and parser configuration.
- [Supported SQL](supported-sql.md) — the breadth of SQL Isthmus translates, driven by
  the TPC-H and TPC-DS test suites.

## API reference

The full Javadoc for every public class is published at
[javadoc.io/doc/io.substrait/isthmus](https://javadoc.io/doc/io.substrait/isthmus).
