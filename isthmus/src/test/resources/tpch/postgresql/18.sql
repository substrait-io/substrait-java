select
  "C"."C_NAME",
  "C"."C_CUSTKEY",
  "O"."O_ORDERKEY",
  "O"."O_ORDERDATE",
  "O"."O_TOTALPRICE",
  sum("L"."L_QUANTITY")
from
  "CUSTOMER" "C",
  "ORDERS" "O",
  "LINEITEM" "L"
where
  "O"."O_ORDERKEY" in (
    select
      "L_ORDERKEY"
    from
      "LINEITEM"
    group by
      "L_ORDERKEY" having
        sum("L_QUANTITY") > 300
  )
  and "C"."C_CUSTKEY" = "O"."O_CUSTKEY"
  and "O"."O_ORDERKEY" = "L"."L_ORDERKEY"
group by
  "C"."C_NAME",
  "C"."C_CUSTKEY",
  "O"."O_ORDERKEY",
  "O"."O_ORDERDATE",
  "O"."O_TOTALPRICE"
order by
  "O"."O_TOTALPRICE" desc,
  "O"."O_ORDERDATE"
limit 100
