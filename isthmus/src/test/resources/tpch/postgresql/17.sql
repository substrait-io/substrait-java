select
  sum("L"."L_EXTENDEDPRICE") / 7.0 as "AVG_YEARLY"
from
  "LINEITEM" "L",
  "PART" "P"
where
  "P"."P_PARTKEY" = "L"."L_PARTKEY"
  and "P"."P_BRAND" = 'Brand#13'
  and "P"."P_CONTAINER" = 'JUMBO CAN'
  and "L"."L_QUANTITY" < (
    select
      0.2 * avg("L2"."L_QUANTITY")
    from
      "LINEITEM" "L2"
    where
      "L2"."L_PARTKEY" = "P"."P_PARTKEY"
  )
