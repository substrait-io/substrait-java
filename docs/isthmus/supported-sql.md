# Supported SQL

Isthmus supports a broad slice of analytical SQL. The clearest measure of that breadth
is the industry-standard benchmark suites that run as tests on every build:

- **All 22 [TPC-H](https://www.tpc.org/tpch/) queries** convert to Substrait and back
  to SQL.
- **Most of the 99 [TPC-DS](https://www.tpc.org/tpcds/) queries** convert; a small
  number use alternate query forms in the suite.

These are exercised query-by-query in `TpchQueryTest` and `TpcdsQueryTest`, which run
each query through the pipeline `SQL -> Substrait POJO -> protobuf -> SQL`. `TpcdsQueryTest`
covers queries 1–99, substituting alternate forms for a handful of them (queries 27, 36,
70, and 86).

!!! note "What the suites assert"
    The TPC-H and TPC-DS tests assert that each query converts through the full pipeline
    *without error*; they do not (yet) assert semantic equivalence of the round-tripped
    plan. Stronger fidelity checks — asserting POJO equality across round trips — are
    made by the plan-level tests such as `Substrait2SqlTest` and `SimplePlansTest`, which
    use the `assertFullRoundTrip` harnesses in `PlanTestBase`.

## Supported categories

Drawn from the test suites, Isthmus translates at least the following:

- **Projection** — column selection, computed columns, aliases.
- **Filtering** — `WHERE` with comparisons, `AND`/`OR`, `IN`, `IS [NOT] NULL`,
  `BETWEEN`, `LIKE`, and `CASE`/`WHEN` expressions.
- **Arithmetic and scalar functions** — numeric, string (`substring`, `lower`, `upper`,
  …), and date/time functions (`extract`, `month`, `year`, `current_timestamp`,
  `current_date`, interval arithmetic).
- **Joins** — inner, `LEFT`, `RIGHT`, and `FULL` outer joins, plus comma (cross) joins.
- **Aggregation** — `GROUP BY`, aggregate functions (`sum`, `count`, `avg`,
  `approx_count_distinct`, …), `DISTINCT` aggregates, `FILTER (WHERE ...)` on aggregates,
  and grouped extensions: `GROUPING SETS`, `ROLLUP`, and `GROUP_ID()`.
- **Window functions** — `OVER (...)` window aggregates.
- **Set operations** — `UNION` / `UNION ALL`, `INTERSECT`, `EXCEPT`, and their
  variants.
- **Ordering and limiting** — `ORDER BY` (with `ASC`/`DESC` and `NULLS FIRST`/`NULLS
  LAST`), `LIMIT`, and `OFFSET`.
- **VALUES / virtual tables** — literal row sets, including `SELECT` with no `FROM`
  (e.g. `SELECT 1`).
- **DDL** — `CREATE TABLE` statements, parsed into a catalog with
  `SubstraitCreateStatementParser` (see [SQL to Substrait](sql-to-substrait.md)).
- **DML** — table modifications (`INSERT`/`UPDATE`/`DELETE`), which map to Substrait
  write relations.

## Example round-trip queries

These are representative queries verified by `Substrait2SqlTest` via
`assertFullRoundTrip`:

```sql
-- Join with arithmetic and a decimal literal
SELECT l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
WHERE l_shipdate < date '1998-01-01';

-- Aggregation with DISTINCT
SELECT l_partkey, count(l_tax), COUNT(distinct l_discount)
FROM lineitem
GROUP BY l_partkey;

-- Grouping sets
SELECT sum(l_discount)
FROM lineitem
GROUP BY grouping sets ((l_orderkey, l_commitdate), l_shipdate);

-- CASE expression
SELECT case when p_size > 100 then 'large'
            when p_size > 50 then 'medium'
            else 'small' end
FROM part;
```

## Extending coverage

Functions and types outside the standard Substrait set are supported by registering
custom functions, dynamic UDFs, or user-defined types. See
[Customization](customization.md).

## Related

- [SQL to Substrait](sql-to-substrait.md) — the conversion entry point.
- [Substrait to SQL](substrait-to-sql.md) — the reverse direction used by the round-trip
  tests.
- [Customization](customization.md) — extending the supported function/type set.
