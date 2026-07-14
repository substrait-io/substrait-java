# SQL expressions

`SqlExpressionToSubstrait` converts standalone SQL expressions — not full queries —
into a Substrait [extended expression](../core/extended-expressions.md). An extended
expression is a self-contained payload that pairs one or more expressions with the
schema (base struct) they are evaluated against, which makes it a convenient way to
push a filter or projection expression to an engine without wrapping it in a plan.

Unlike [`SqlToSubstrait`](sql-to-substrait.md), this converter returns the **protobuf**
type `io.substrait.proto.ExtendedExpression` directly.

## The `convert` methods

```java
public io.substrait.proto.ExtendedExpression convert(
    String sqlExpression, List<String> createStatements) throws SqlParseException;

public io.substrait.proto.ExtendedExpression convert(
    String[] sqlExpressions, List<String> createStatements) throws SqlParseException;
```

- `sqlExpression` / `sqlExpressions` — one or more SQL expressions (not `SELECT`
  statements), for example `L_ORDERKEY > 10`.
- `createStatements` — a list of `CREATE TABLE` statements that define the columns the
  expressions may reference. Their columns become the base schema; referencing a column
  not present in the schema fails validation.

Each converted expression is added as an `ExpressionReference` with a generated output
name (`column-1`, `column-2`, …), and the combined base schema (a Substrait
`NamedStruct`) is attached to the result.

!!! warning "Column names must be unique"
    The columns from all the `createStatements` are flattened into a single name space
    used to bind field references. Two columns with the same name (across one or more
    tables) raise `IllegalArgumentException: There is no support for duplicate column
    names`.

## Example

Define a schema, then convert a single expression:

```java
import io.substrait.isthmus.SqlExpressionToSubstrait;
import io.substrait.proto.ExtendedExpression;
import java.util.List;

List<String> schema =
    List.of("CREATE TABLE lineitem (L_ORDERKEY BIGINT, L_COMMENT VARCHAR)");

ExtendedExpression expr =
    new SqlExpressionToSubstrait().convert("L_ORDERKEY > 10", schema);
```

The expression `L_ORDERKEY > 10` becomes a Substrait scalar-function call
(`greater_than`) over a field reference and an `i32` literal, carried in an
`ExtendedExpression` whose base schema has the `L_ORDERKEY` and `L_COMMENT` fields.

## Supported expression kinds

The following categories all convert (drawn directly from the Isthmus test suite):

```sql
2                        -- literal
L_ORDERKEY               -- field reference
L_ORDERKEY > 10          -- comparison (scalar function)
L_ORDERKEY + 10          -- arithmetic (scalar function)
L_ORDERKEY IN (10, 20)   -- IN
L_ORDERKEY IS NOT NULL   -- IS NOT NULL
L_ORDERKEY IS NULL       -- IS NULL
```

## Converting several expressions at once

Pass an array to bundle multiple expressions into one `ExtendedExpression`. They share
the same base schema and are named `column-1`, `column-2`, … in order:

```java
String[] expressions = {
  "L_ORDERKEY",
  "L_ORDERKEY > 10",
  "L_ORDERKEY + 10",
  "L_ORDERKEY IN (10, 20)",
  "L_ORDERKEY IS NOT NULL"
};

ExtendedExpression expr = new SqlExpressionToSubstrait().convert(expressions, schema);
```

## Related

- [core extended expressions](../core/extended-expressions.md) — the extended
  expression model and its POJO <-> protobuf serialization.
- [SQL to Substrait](sql-to-substrait.md) — convert whole SQL statements into a `Plan`.
