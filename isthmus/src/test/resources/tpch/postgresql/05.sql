select
  "N"."N_NAME",
  sum("L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT")) as "REVENUE"

from
  "CUSTOMER" "C",
  "ORDERS" "O",
  "LINEITEM" "L",
  "SUPPLIER" "S",
  "NATION" "N",
  "REGION" "R"

where
  "C"."C_CUSTKEY" = "O"."O_CUSTKEY"
  and "L"."L_ORDERKEY" = "O"."O_ORDERKEY"
  and "L"."L_SUPPKEY" = "S"."S_SUPPKEY"
  and "C"."C_NATIONKEY" = "S"."S_NATIONKEY"
  and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
  and "N"."N_REGIONKEY" = "R"."R_REGIONKEY"
  and "R"."R_NAME" = 'EUROPE'
  and "O"."O_ORDERDATE" >= date '1997-01-01'
  and "O"."O_ORDERDATE" < date '1997-01-01' + interval '1 year'
group by
  "N"."N_NAME"

order by
  "REVENUE" desc
