# Substrait to SQL

`SubstraitToSql` renders a Substrait plan back into SQL. It first converts the plan to
a Calcite `RelNode` tree (see [Substrait to Calcite](substrait-to-calcite.md)) and then
uses Calcite's `RelToSqlConverter` to emit SQL text in the dialect you choose.

## The `convert` method

```java
public List<String> convert(Plan plan, SqlDialect dialect);
```

- `plan` — the Substrait POJO `io.substrait.plan.Plan` to render.
- `dialect` — the Calcite `SqlDialect` that controls the SQL that is generated
  (keyword casing, quoting, function rendering, and so on).

The method returns a `List<String>` with **one SQL statement per `Plan.Root`**, in
order. Each root is converted to Calcite, projected onto its final field names, and
serialized with the given dialect.

## Example

```java
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.isthmus.sql.SubstraitSqlDialect;
import io.substrait.plan.Plan;
import java.util.List;

Plan plan = /* a Substrait plan, e.g. from SqlToSubstrait or a proto */;

List<String> sql = new SubstraitToSql().convert(plan, SubstraitSqlDialect.DEFAULT);
String firstStatement = sql.get(0);
```

Any Calcite `SqlDialect` works. `SubstraitSqlDialect.DEFAULT` is the Isthmus dialect
used internally; to target a specific engine, pass one of Calcite's built-in dialects
(for example `org.apache.calcite.sql.dialect.SparkSqlDialect.DEFAULT`). To change how
individual operators are rendered per engine, supply a custom dialect — see
[Customization](customization.md).

## Round trip from a proto plan

`SubstraitToSql` operates on the POJO `Plan`, so a plan received as protobuf must first
be read into a POJO with `ProtoPlanConverter` (from `:core`). The full
POJO -> proto -> POJO -> SQL round trip looks like this:

```java
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.isthmus.SubstraitToSql;
import io.substrait.isthmus.sql.SubstraitSqlDialect;

// POJO Plan -> protobuf
io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);

// protobuf -> POJO Plan
Plan restored = new ProtoPlanConverter().from(proto);

// POJO Plan -> SQL
List<String> sql = new SubstraitToSql().convert(restored, SubstraitSqlDialect.DEFAULT);
```

See [core serialization](../core/serialization.md) for the POJO <-> protobuf converters
in detail.

!!! note "Lossy by nature"
    Substrait is a lower-level algebra than SQL, and Calcite's `RelToSqlConverter`
    reconstructs *a* SQL statement with the same semantics rather than the original
    query text. Expect differences in aliases, parenthesization, and the exact shape of
    the generated SQL.

## Converting a single relation to Calcite

If you only need the Calcite side, `substraitRelToCalciteRel` converts a Substrait
`Rel` to a Calcite `RelNode` given a catalog:

```java
public RelNode substraitRelToCalciteRel(Rel relRoot, Prepare.CatalogReader catalog);
```

For converting whole plan roots to Calcite `RelRoot`s, use
[`SubstraitToCalcite`](substrait-to-calcite.md) directly.
