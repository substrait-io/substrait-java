# Supported features

The converters translate the common core of Spark's logical model — enough that **every TPC-H
query round-trips** through Substrait and back. Coverage is not exhaustive, though: **TPC-DS has
known gaps** that prevent some of its queries from being translated, and any unsupported node,
type, or function raises `UnsupportedOperationException` (or a resolution error) rather than being
silently dropped.

This page summarizes what is supported. The type and function mappings are declared in the Spark
dialect (`spark/spark_dialect.yaml`), and the Spark ⇄ Substrait function signatures are wired up in
`FunctionMappings`.

## Types

Substrait types map to Spark SQL types as follows (both directions):

| Substrait type | Spark type |
| --- | --- |
| `I8` | `ByteType` |
| `I16` | `ShortType` |
| `I32` | `IntegerType` |
| `I64` | `LongType` |
| `FP32` | `FloatType` |
| `FP64` | `DoubleType` |
| `DECIMAL` | `DecimalType` |
| `DATE` | `DateType` |
| `STRING` | `StringType` |
| `VARCHAR` | `StringType` |
| `FIXED_CHAR` | `StringType` |
| `BINARY` | `BinaryType` |
| `BOOL` | `BooleanType` |
| `PRECISION_TIMESTAMP` (max precision 9) | `TimestampNTZType` |
| `PRECISION_TIMESTAMP_TZ` (max precision 9) | `TimestampType` |
| `INTERVAL_DAY` (max precision 9) | `DayTimeIntervalType` |
| `INTERVAL_YEAR` | `YearMonthIntervalType` |
| `LIST` | `ArrayType` |
| `MAP` | `MapType` |
| `STRUCT` | `StructType` |

## Expressions

- **Literals**
- **Field references** (selections)
- **Scalar functions** (see [Functions](#functions))
- **`IF`/`CASE`** (if-then)
- **`IN` lists** (singular-or-list)
- **Casts**
- **Subqueries** — scalar subqueries and `IN` predicate (semi-join) subqueries

## Relations

- **Project**
- **Filter**
- **Aggregate** — a single grouping set; `Rollup`/`GroupingSets` are not yet supported. A `Project`
  is layered on top so grouping keys and aggregate results can be reordered and combined with
  scalar expressions.
- **Sort**
- **Fetch** — `LIMIT` and `OFFSET` (and their combination)
- **Join** — `INNER`, `LEFT`/`RIGHT` outer, `OUTER` (full), `LEFT_SEMI`, `LEFT_ANTI`. Spark's
  internal `ExistenceJoin` is modelled with a Substrait `InPredicate` inside a filter.
- **Cross** — a cross/Cartesian join (an inner join with a trivially-true condition is emitted as a
  `Cross`)
- **Set** — `UNION ALL` (union-by-name is not supported)
- **Window** — consistent-partition window functions (see [Functions](#functions))
- **Expand** — for multi-projection expansions (switching-field form)
- **Read** — virtual tables (in-memory/`LocalRelation`), local files (CSV, Parquet, ORC), and
  named tables (catalog scans)
- **Write** — insert into files or Hive tables, and CTAS (create-table-as-select)
- **DDL** — `CREATE TABLE` / `DROP TABLE` on named objects

!!! note "File formats"
    File-backed reads and writes support **CSV, Parquet, and ORC**. CSV carries its delimiter,
    quote, escape, header, and null-value options through the plan. Other formats raise
    `UnsupportedOperationException`.

## Functions

Function names below are the Substrait function names; each maps to the corresponding Spark
Catalyst expression.

### Scalar functions

- **Arithmetic:** `add`, `subtract`, `multiply`, `divide`, `abs`, `modulus`, `power`, `exp`,
  `sqrt`, `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `atan2`, `sinh`, `cosh`, `tanh`, `asinh`,
  `acosh`, `atanh`
- **Logarithmic:** `ln`, `log10`
- **Rounding:** `round`, `floor`, `ceil`
- **Boolean:** `and`, `or`, `not`
- **Comparison:** `equal`, `is_not_distinct_from`, `lt`, `lte`, `gt`, `gte`, `is_null`,
  `is_not_null`
- **String:** `substring`, `upper`, `lower`, `lpad`, `rpad`, `concat`, `like`, `contains`,
  `starts_with`, `ends_with`, `trim`, `ltrim`, `rtrim`
- **Null handling:** `coalesce`
- **Bitwise:** `bitwise_and`, `bitwise_or`, `bitwise_xor`, `shift_left`, `shift_right`,
  `shift_right_unsigned`
- **Date/time:** `date_add` (Spark extension), and `extract` — Spark's `Year`, `Quarter`, `Month`,
  and `DayOfMonth` map to the Substrait `extract` function with the appropriate enum argument

### Aggregate functions

`sum`, `avg`, `count`, `min`, `max`, `any_value` (Spark `First`), `approx_count_distinct`
(HyperLogLog++), and `std_dev` (sample standard deviation).

### Window functions

`row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `ntile`, `lead`, `lag`, and
`nth_value`.

## Benchmark coverage

- **TPC-H:** all queries round-trip Spark → Substrait → Spark.
- **TPC-DS:** most queries work, but there are **known gaps** — some queries use constructs
  (such as rollups/grouping sets) that are not yet supported and therefore do not translate.
