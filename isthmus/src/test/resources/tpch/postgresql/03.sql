select
  "L"."L_ORDERKEY",
  sum("L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT")) as "REVENUE",
  "O"."O_ORDERDATE",
  "O"."O_SHIPPRIORITY"

from
  "CUSTOMER" "C",
  "ORDERS" "O",
  "LINEITEM" "L"

where
  "C"."C_MKTSEGMENT" = 'HOUSEHOLD'
  and "C"."C_CUSTKEY" = "O"."O_CUSTKEY"
  and "L"."L_ORDERKEY" = "O"."O_ORDERKEY"
  and "O"."O_ORDERDATE" < date '1995-03-25'
  and "L"."L_SHIPDATE" > date '1995-03-25'

group by
  "L"."L_ORDERKEY",
  "O"."O_ORDERDATE",
  "O"."O_SHIPPRIORITY"
order by
  "REVENUE" desc,
  "O"."O_ORDERDATE"
limit 10
