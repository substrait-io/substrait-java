select
  "NATION",
  "O_YEAR",
  sum("AMOUNT") as "SUM_PROFIT"
from
  (
    select
      "N"."N_NAME" as "NATION",
      extract(year from "O"."O_ORDERDATE") as "O_YEAR",
      "L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT") - "PS"."PS_SUPPLYCOST" * "L"."L_QUANTITY" as "AMOUNT"
    from
      "PART" "P",
      "SUPPLIER" "S",
      "LINEITEM" "L",
      "PARTSUPP" "PS",
      "ORDERS" "O",
      "NATION" "N"
    where
      "S"."S_SUPPKEY" = "L"."L_SUPPKEY"
      and "PS"."PS_SUPPKEY" = "L"."L_SUPPKEY"
      and "PS"."PS_PARTKEY" = "L"."L_PARTKEY"
      and "P"."P_PARTKEY" = "L"."L_PARTKEY"
      and "O"."O_ORDERKEY" = "L"."L_ORDERKEY"
      and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
      and "P"."P_NAME" like '%yellow%'
  ) as "PROFIT"
group by
  "NATION",
  "O_YEAR"
order by
  "NATION",
  "O_YEAR" desc
