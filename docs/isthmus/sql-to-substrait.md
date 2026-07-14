# SQL to Substrait

`SqlToSubstrait` converts one or more SQL statements into a Substrait plan. Because
SQL references tables by name, the converter needs a *catalog* describing those tables.
Isthmus builds that catalog from `CREATE TABLE` statements, so a full conversion has
two steps: parse the schema into a catalog, then convert the query against it.

The result is a substrait-java POJO `io.substrait.plan.Plan` — the same immutable model
described in the [core docs](../core/index.md). Serialize it to the protobuf wire
format when you need to send or persist it.

## The `convert` methods

```java
public Plan convert(String sqlStatements, Prepare.CatalogReader catalogReader)
    throws SqlParseException;

public Plan convert(String sqlStatements, Prepare.CatalogReader catalogReader,
    SqlDialect sqlDialect) throws SqlParseException;
```

- `sqlStatements` — a string containing one or more SQL statements (separate multiple
  statements with `;`).
- `catalogReader` — a Calcite `Prepare.CatalogReader` describing the tables the SQL
  references (see below).
- `sqlDialect` — optional; supply a Calcite `SqlDialect` to control how the SQL is
  *parsed* (for example a dialect's identifier-quoting and casing rules). Without it,
  Isthmus uses the parser configuration from the `ConverterProvider`.

Every root query in the input becomes one `Plan.Root`; the converter tags the plan with
a version whose producer is `"isthmus"`.

## Building a catalog from CREATE statements

`SubstraitCreateStatementParser.processCreateStatementsToCatalog(...)` parses SQL
`CREATE TABLE` statements and returns a `CalciteCatalogReader` you can hand straight to
`convert`. It accepts either a varargs of strings or a `List<String>`:

```java
public static CalciteCatalogReader processCreateStatementsToCatalog(String... createStatements)
    throws SqlParseException;

public static CalciteCatalogReader processCreateStatementsToCatalog(List<String> createStatements)
    throws SqlParseException;
```

Each string may itself contain several `CREATE TABLE` statements. Only `CREATE TABLE`
is accepted — `CREATE TABLE ... AS SELECT` (CTAS) is rejected. Primary-key and other
key constraints are parsed and ignored, so they are safe to include.

## Worked example

Define a schema, convert a query, and serialize the resulting plan to protobuf:

```java
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import org.apache.calcite.prepare.Prepare;

String[] createStatements = {
  "CREATE TABLE users (id BIGINT, name VARCHAR, signup_date DATE)",
  "CREATE TABLE orders (order_id BIGINT, user_id BIGINT, total DECIMAL(10, 2))"
};

// 1. Parse the schema into a catalog.
Prepare.CatalogReader catalog =
    SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatements);

// 2. Convert a query into a Substrait Plan POJO.
Plan plan =
    new SqlToSubstrait()
        .convert(
            "SELECT u.name, o.total "
                + "FROM users u JOIN orders o ON u.id = o.user_id "
                + "WHERE o.total > 100.00",
            catalog);

// 3. Serialize the POJO Plan to the protobuf wire format.
io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
```

`plan` is a fully-formed Substrait plan you can inspect, transform, or (as shown)
serialize. Converting the POJO `Plan` to protobuf is a core concern rather than an
Isthmus one — `PlanProtoConverter` lives in `:core`. See
[core serialization](../core/serialization.md) for the round trip and for reading a
proto plan back into a POJO with `ProtoPlanConverter`.

!!! note "Identifier casing"
    With the default parser configuration, unquoted identifiers are upper-cased and
    table/column lookups are case-insensitive, so `users` and `USERS` resolve to the
    same table. Quote identifiers to preserve their exact case. The parser behavior is
    configurable via the `ConverterProvider`; see [Customization](customization.md).

## Multiple statements

Passing several statements in one call produces a plan with one root per statement:

```java
Plan plan =
    new SqlToSubstrait()
        .convert(
            "SELECT order_id FROM orders; "
                + "SELECT user_id FROM orders WHERE total > 20;",
            catalog);

// plan.getRoots() has two entries, one per SELECT.
```

A trailing semicolon on a single statement is fine, and each statement is converted
independently.

## Related

- [SQL expressions](sql-expressions.md) — convert a bare SQL expression (rather than a
  full query) into a Substrait `ExtendedExpression`.
- [Substrait to SQL](substrait-to-sql.md) — go the other way and render a plan back to
  SQL.
- [core serialization](../core/serialization.md) — POJO `Plan` <-> protobuf.
