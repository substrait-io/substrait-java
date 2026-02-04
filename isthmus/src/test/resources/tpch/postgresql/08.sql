select
  "O_YEAR",
  sum(case
    when "NATION" = 'EGYPT' then "VOLUME"
    else 0
  end) / sum("VOLUME") as "MKT_SHARE"
from
  (
    select
      extract(year from "O"."O_ORDERDATE") as "O_YEAR",
      "L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT") as "VOLUME",
      "N2"."N_NAME" as "NATION"
    from
      "PART" "P",
      "SUPPLIER" "S",
      "LINEITEM" "L",
      "ORDERS" "O",
      "CUSTOMER" "C",
      "NATION" "N1",
      "NATION" "N2",
      "REGION" "R"
    where
      "P"."P_PARTKEY" = "L"."L_PARTKEY"
      and "S"."S_SUPPKEY" = "L"."L_SUPPKEY"
      and "L"."L_ORDERKEY" = "O"."O_ORDERKEY"
      and "O"."O_CUSTKEY" = "C"."C_CUSTKEY"
      and "C"."C_NATIONKEY" = "N1"."N_NATIONKEY"
      and "N1"."N_REGIONKEY" = "R"."R_REGIONKEY"
      and "R"."R_NAME" = 'MIDDLE EAST'
      and "S"."S_NATIONKEY" = "N2"."N_NATIONKEY"
      and "O"."O_ORDERDATE" between date '1995-01-01' and date '1996-12-31'
      and "P"."P_TYPE" = 'PROMO BRUSHED COPPER'
  ) as "ALL_NATIONS"
group by
  "O_YEAR"
order by
  "O_YEAR"
