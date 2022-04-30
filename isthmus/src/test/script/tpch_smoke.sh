#!/bin/bash
parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "${parent_path}"
CMD=../../../build/graal/isthmus

TPCH="../resources/tpch/"

DDL=`cat ${TPCH}/schema.sql`
QUERY_FOLDER="${TPCH}/queries"

# echo $DDL

for i in {1..5}; do echo $i; done

for querynum in {1..22}; do
     if [ $querynum -lt 10 ]; then
       querynumstr="0${querynum}"
     else
       querynumstr="${querynum}"
    fi

    QUERY=`cat ${QUERY_FOLDER}/${querynumstr}.sql`
    echo "Processing tpc-h query", $querynum
    echo $QUERY
    $CMD "${QUERY}" --create "${DDL}"
done

## $CMD 'select l_orderkey from lineitem' --create "${DDL}"
