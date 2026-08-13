#!/bin/bash

set -eu -o pipefail

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "${parent_path}/../../.."
CMD="${ISTHMUS:-build/native/nativeCompile/isthmus}"
LINEITEM='CREATE TABLE LINEITEM (L_ORDERKEY BIGINT NOT NULL, L_PARTKEY BIGINT NOT NULL, L_SUPPKEY BIGINT NOT NULL, L_LINENUMBER INTEGER, L_QUANTITY DECIMAL, L_EXTENDEDPRICE DECIMAL, L_DISCOUNT DECIMAL, L_TAX DECIMAL, L_RETURNFLAG CHAR(1), L_LINESTATUS CHAR(1), L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT CHAR(25), L_SHIPMODE CHAR(10), L_COMMENT VARCHAR(44))'
echo "${LINEITEM}"
#set -x

# SQL Query - Simple
"${CMD}" 'select * from lineitem' --create "${LINEITEM}"

# SQL Query - With condition
"${CMD}" 'select * from lineitem where l_orderkey > 10' --create "${LINEITEM}"

# SQL Query - Aggregate
"${CMD}" 'select l_orderkey, count(l_partkey) from lineitem group by l_orderkey' --create "${LINEITEM}"

# SQL Query - Grouping-only aggregate (no aggregate function; exercises the column-uniqueness
# metadata query that must not fall back to Janino runtime codegen in the native image)
"${CMD}" 'select l_orderkey from lineitem group by l_orderkey' --create "${LINEITEM}"

# SQL Query - DISTINCT (same metadata-query path as grouping-only aggregate)
"${CMD}" 'select distinct l_orderkey from lineitem' --create "${LINEITEM}"

# SQL Expression - Literal expression
"${CMD}" --expression '10'

# SQL Expression - Reference expression
"${CMD}" --expression 'l_suppkey' --create "${LINEITEM}"

# SQL Expression - Filter expression
"${CMD}" --expression 'l_orderkey > 10' --create "${LINEITEM}"

# SQL Expression - Projection expression (column-1)
"${CMD}" --expression 'l_orderkey + 9888486986' --create "${LINEITEM}"

# SQL Expression - 03 Projection expression (column-1, column-2, column-3)
"${CMD}" --expression 'l_orderkey + 9888486986' 'l_orderkey * 2' 'l_orderkey > 10' 'l_orderkey in (10, 20)' --create "${LINEITEM}"

# Error path - a query over an undefined table must be reported as a message hinting at --create,
# not as a Java stack trace
if error=$("${CMD}" 'select * from lineitem' 2>&1); then
  echo "expected a query without a table definition to fail"
  exit 1
fi
echo "${error}"
# Match the hint itself rather than the option name, which also appears in the usage block
if ! grep -q 'table definitions are not part of the query' <<<"${error}"; then
  echo "expected a hint about --create"
  exit 1
fi
if grep -q $'\tat ' <<<"${error}"; then
  echo "expected a message instead of a stack trace"
  exit 1
fi

# Error path - a syntax error must be reported at its position. Calcite collects the tokens the
# grammar expected there by reflecting on the parser, which only works in the native image if those
# productions were registered, so this is not covered by the JVM tests.
if syntax=$("${CMD}" 'select 1 from' 2>&1); then
  echo "expected a malformed query to fail"
  exit 1
fi
echo "${syntax}"
if ! grep -q 'Encountered "<EOF>" at line 1, column 13' <<<"${syntax}"; then
  echo "expected the parse error to name the position"
  exit 1
fi
if grep -q $'\tat ' <<<"${syntax}"; then
  echo "expected a message instead of a stack trace"
  exit 1
fi

# Error path - --stacktrace keeps the trace as well as the message
if trace=$("${CMD}" --stacktrace 'select * from lineitem' 2>&1); then
  echo "expected a query without a table definition to fail"
  exit 1
fi
if ! grep -q 'table definitions are not part of the query' <<<"${trace}"; then
  echo "expected --stacktrace to print the hint as well"
  exit 1
fi
if ! grep -q $'\tat ' <<<"${trace}"; then
  echo "expected --stacktrace to print the stack trace"
  exit 1
fi
