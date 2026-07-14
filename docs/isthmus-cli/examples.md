# Examples

Worked examples of converting SQL to Substrait with the `isthmus` binary. The commands use
the built binary path (`./isthmus-cli/build/native/nativeCompile/isthmus`); substitute your
own binary location as needed — see [Install & build](install.md).

Outputs below are `PROTOJSON` (the default) and are **abbreviated** — long, repeated
sections are elided with a `// ...` comment. See [Usage](usage.md) for the full flag
reference.

## SQL query to a Substrait Plan

Declare the catalog with `-c`/`--create`, then pass the query as the positional argument.
The tool prints a Substrait `Plan`:

```bash
./isthmus-cli/build/native/nativeCompile/isthmus \
  -c "CREATE TABLE Persons ( firstName VARCHAR, lastName VARCHAR, zip INT )" \
     "SELECT lastName, firstName FROM Persons WHERE zip = 90210"
```

```json
{
  "extensionUrns": [{
    "extensionUrnAnchor": 1,
    "urn": "extension:io.substrait:functions_comparison"
  }],
  "extensions": [{
    "extensionFunction": {
      "functionAnchor": 0,
      "name": "equal:any1_any1",
      "extensionUrnReference": 1
    }
  }],
  "relations": [{
    "root": {
      "input": {
        "project": {
          "common": { "emit": { "outputMapping": [3, 4] } },
          "input": {
            "filter": {
              "input": {
                "read": {
                  "baseSchema": {
                    "names": ["FIRSTNAME", "LASTNAME", "ZIP"]
                    // ... struct types elided ...
                  },
                  "namedTable": { "names": ["PERSONS"] }
                }
              },
              "condition": {
                "scalarFunction": {
                  "functionReference": 0,
                  "args": [
                    { "selection": { "directReference": { "structField": { "field": 2 } } } },
                    { "literal": { "i32": 90210 } }
                  ]
                  // ... outputType elided ...
                }
              }
            }
          },
          "expressions": [
            { "selection": { "directReference": { "structField": { "field": 1 } } } },
            { "selection": { "directReference": { "structField": { "field": 0 } } } }
          ]
        }
      },
      "names": ["LASTNAME", "FIRSTNAME"]
    }
  }],
  "expectedTypeUrls": []
}
```

!!! note "Uppercased names"
    The table and column names appear upper-cased (`PERSONS`, `FIRSTNAME`) because the
    default `--unquotedcasing` policy is `TO_UPPER`. See [Usage](usage.md#unquoted-identifier-casing).

## SQL expression to a Substrait ExtendedExpression

Adding `-e`/`--expression` switches the tool to expression mode: it converts each
expression against the catalog and prints an `ExtendedExpression`.

### Projection expression

```bash
./isthmus-cli/build/native/nativeCompile/isthmus \
  -c "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))" \
  -e "N_REGIONKEY + 10"
```

```json
{
  "extensionUrns": [{
    "extensionUrnAnchor": 1,
    "urn": "extension:io.substrait:functions_arithmetic"
  }],
  "extensions": [{
    "extensionFunction": {
      "functionAnchor": 0,
      "name": "add:i64_i64",
      "extensionUrnReference": 1
    }
  }],
  "referredExpr": [{
    "expression": {
      "scalarFunction": {
        "functionReference": 0,
        "outputType": { "i64": { "nullability": "NULLABILITY_REQUIRED" } },
        "arguments": [
          { "value": { "selection": { "directReference": { "structField": { "field": 2 } } } } },
          { "value": { "cast": { "type": { "i64": {} }, "input": { "literal": { "i32": 10 } } } } }
        ]
      }
    },
    "outputNames": ["new-column"]
  }],
  "baseSchema": {
    "names": ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"]
    // ... struct types elided ...
  },
  "expectedTypeUrls": []
}
```

### Filter expression

A boolean expression produces the same `ExtendedExpression` shape, with a comparison
function extension:

```bash
./isthmus-cli/build/native/nativeCompile/isthmus \
  -c "CREATE TABLE NATION (N_NATIONKEY BIGINT NOT NULL, N_NAME CHAR(25), N_REGIONKEY BIGINT NOT NULL, N_COMMENT VARCHAR(152))" \
  -e "N_REGIONKEY > 10"
```

```json
{
  "extensionUrns": [{
    "extensionUrnAnchor": 1,
    "urn": "extension:io.substrait:functions_comparison"
  }],
  "extensions": [{
    "extensionFunction": {
      "functionAnchor": 0,
      "name": "gt:any_any",
      "extensionUrnReference": 1
    }
  }],
  "referredExpr": [{
    "expression": {
      "scalarFunction": {
        "functionReference": 0,
        "outputType": { "bool": { "nullability": "NULLABILITY_REQUIRED" } },
        "arguments": [
          { "value": { "selection": { "directReference": { "structField": { "field": 2 } } } } },
          { "value": { "cast": { "type": { "i64": {} }, "input": { "literal": { "i32": 10 } } } } }
        ]
      }
    },
    "outputNames": ["new-column"]
  }],
  "baseSchema": {
    "names": ["N_NATIONKEY", "N_NAME", "N_REGIONKEY", "N_COMMENT"]
    // ... struct types elided ...
  },
  "expectedTypeUrls": []
}
```

!!! tip "Multiple expressions at once"
    Because `-e` has arity `1..*`, you can convert several expressions in one call — each
    becomes an entry in `referredExpr`:

    ```bash
    ./isthmus-cli/build/native/nativeCompile/isthmus \
      -c "$LINEITEM" \
      -e 'l_orderkey + 9888486986' 'l_orderkey * 2' 'l_orderkey > 10' 'l_orderkey in (10, 20)'
    ```

## Smoke-test scripts

The module ships two runnable scripts that exercise the binary against a range of inputs —
handy as ready-made examples. Both resolve the binary from the `ISTHMUS` environment
variable, defaulting to `build/native/nativeCompile/isthmus` (relative to the `isthmus-cli`
module), so set `ISTHMUS` to run them against a downloaded release binary.

### `smoke.sh`

Runs a spread of queries and expressions against a single `LINEITEM` table — simple
`SELECT`, a filter, an aggregate, and literal / reference / filter / projection
expressions (including converting several expressions in one call):

```bash
./isthmus-cli/src/test/script/smoke.sh
```

### `tpch_smoke.sh`

Generates Substrait plans for all 22 TPC-H queries, using the schema and query files from
the `isthmus` module's test resources:

```bash
./isthmus-cli/src/test/script/tpch_smoke.sh
```

This is a good way to see what non-trivial plans look like across a well-known query set.
