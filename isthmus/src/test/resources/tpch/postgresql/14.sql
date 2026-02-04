select
  100.00 * sum(case
    when "P"."P_TYPE" like 'PROMO%'
      then "L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT")
    else 0
  end) / sum("L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT")) as "PROMO_REVENUE"
from
  "LINEITEM" "L",
  "PART" "P"
where
  "L"."L_PARTKEY" = "P"."P_PARTKEY"
  and "L"."L_SHIPDATE" >= date '1994-08-01'
  and "L"."L_SHIPDATE" < date '1994-08-01' + interval '1 month'
