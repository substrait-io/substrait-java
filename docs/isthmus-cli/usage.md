# Usage

The `isthmus` command converts SQL queries and SQL expressions to Substrait. This page
documents every flag and argument, their defaults, and how the tool decides what to
produce.

The examples below use the built binary path; substitute your own binary location (for
example a downloaded release binary on your `PATH`) as needed. See
[Install & build](install.md).

## General form

```bash
isthmus [-hV] [--outputformat=<outputFormat>] [--unquotedcasing=<unquotedCasing>] \
        [-c=<createStatement>]... [-e=<sqlExpression>...]... [<sql>]
```

Running `isthmus` with no arguments (and `-h`/`--help`) prints this usage help.

## Arguments and options

### Positional argument

| Argument | Arity | Description |
| --- | --- | --- |
| `<sql>` | `0..1` | A single SQL query to convert to a Substrait `Plan`. |

Because a query string often starts with words that could look like options, use the
`--` separator to mark the end of options when the query follows other flags:

```bash
isthmus --create "$DDL" -- "SELECT * FROM lineitem"
```

### Options

| Option | Arity | Default | Description |
| --- | --- | --- | --- |
| `-e`, `--expression=<sqlExpression>...` | `1..*` | — | One or more SQL expressions, e.g. `col + 1`. When present, the tool converts expressions instead of a query. |
| `-c`, `--create=<createStatement>` | repeatable | none | One or more `CREATE TABLE` statements defining the catalog, e.g. `CREATE TABLE T1(foo int, bar bigint)`. Repeat the flag to declare multiple tables. |
| `--outputformat=<outputFormat>` | 1 | `PROTOJSON` | Output format for the generated message: `PROTOJSON`, `PROTOTEXT`, or `BINARY`. |
| `--unquotedcasing=<unquotedCasing>` | 1 | `TO_UPPER` | Calcite's casing policy for unquoted identifiers: `UNCHANGED`, `TO_UPPER`, or `TO_LOWER`. |
| `-h`, `--help` | — | — | Show the help message and exit. |
| `-V`, `--version` | — | — | Print version information and exit. |

!!! note "Enum values are case-insensitive"
    Values for `--outputformat` and `--unquotedcasing` are matched case-insensitively, so
    `--outputformat prototext` and `--outputformat PROTOTEXT` are equivalent.

## What gets produced

The tool operates in one of two modes, decided by whether `-e`/`--expression` is present:

- **Expression mode** — if one or more `-e`/`--expression` values are given, the tool
  converts them (against the catalog built from any `-c` statements) and prints a Substrait
  `ExtendedExpression`. The `<sql>` positional argument is not used in this mode.
- **Query mode** (default) — otherwise, the tool builds a catalog from the `-c`/`--create`
  statements and converts the `<sql>` query, printing a Substrait `Plan`.

## Output formats

`--outputformat` controls how the resulting protobuf message is serialized to standard
output:

- **`PROTOJSON`** (default) — protobuf JSON. Default-valued fields are included, so the
  output is verbose (you will see empty arrays such as `"expectedTypeUrls": []` and
  zero-valued fields like `"typeVariationReference": 0`). This is the most readable format
  for inspecting plans.
- **`PROTOTEXT`** — protobuf text format.
- **`BINARY`** — the raw protobuf binary wire format, written to stdout. Redirect it to a
  file (for example `> plan.pb`) when you want to consume it programmatically.

## Unquoted identifier casing

`--unquotedcasing` maps directly to Calcite's SQL parser policy for identifiers that are
not double-quoted:

- **`TO_UPPER`** (default) — unquoted identifiers are folded to upper case. This is why, in
  the [examples](examples.md), a table declared as `Persons` with a column `firstName`
  appears in the plan as `PERSONS` / `FIRSTNAME`.
- **`TO_LOWER`** — folded to lower case.
- **`UNCHANGED`** — preserved exactly as written.

Choose the policy that matches the SQL dialect and catalog naming you are targeting.
