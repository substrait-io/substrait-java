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
