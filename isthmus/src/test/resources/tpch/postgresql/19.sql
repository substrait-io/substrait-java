select
  sum("L"."L_EXTENDEDPRICE"* (1 - "L"."L_DISCOUNT")) as "REVENUE"
from
  "LINEITEM" "L",
  "PART" "P"
where
  (
    "P"."P_PARTKEY" = "L"."L_PARTKEY"
    and "P"."P_BRAND" = 'Brand#41'
    and "P"."P_CONTAINER" in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    and "L"."L_QUANTITY" >= 2 and "L"."L_QUANTITY" <= 2 + 10
    and "P"."P_SIZE" between 1 and 5
    and "L"."L_SHIPMODE" in ('AIR', 'AIR REG')
    and "L"."L_SHIPINSTRUCT" = 'DELIVER IN PERSON'
  )
  or
  (
    "P"."P_PARTKEY" = "L"."L_PARTKEY"
    and "P"."P_BRAND" = 'Brand#13'
    and "P"."P_CONTAINER" in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    and "L"."L_QUANTITY" >= 14 and "L"."L_QUANTITY" <= 14 + 10
    and "P"."P_SIZE" between 1 and 10
    and "L"."L_SHIPMODE" in ('AIR', 'AIR REG')
    and "L"."L_SHIPINSTRUCT" = 'DELIVER IN PERSON'
  )
  or
  (
    "P"."P_PARTKEY" = "L"."L_PARTKEY"
    and "P"."P_BRAND" = 'Brand#55'
    and "P"."P_CONTAINER" in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    and "L"."L_QUANTITY" >= 23 and "L"."L_QUANTITY" <= 23 + 10
    and "P"."P_SIZE" between 1 and 15
    and "L"."L_SHIPMODE" in ('AIR', 'AIR REG')
    and "L"."L_SHIPINSTRUCT" = 'DELIVER IN PERSON'
  )
