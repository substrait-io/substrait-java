#!/bin/bash

set -eu -o pipefail

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "${parent_path}"
CMD=../../../build/native/nativeCompile/isthmus

TPCH="../../../../isthmus/src/test/resources/tpch"

DDL=$(cat ${TPCH}/schema.sql)
QUERY_FOLDER="${TPCH}/queries"

for QUERY_NUM in {1..22}; do
    if [ "${QUERY_NUM}" -lt 10 ]; then
       QUERY=$(cat "${QUERY_FOLDER}/0${QUERY_NUM}.sql")
     else
       QUERY=$(cat "${QUERY_FOLDER}/${QUERY_NUM}.sql")
    fi

    echo "Processing tpc-h query ${QUERY_NUM}"
    echo "${QUERY}"
    $CMD --create "${DDL}" -- "${QUERY}"
done
