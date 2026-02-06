select
  "SUPP_NATION",
  "CUST_NATION",
  "L_YEAR",
  sum("VOLUME") as "REVENUE"
from
  (
    select
      "N1"."N_NAME" as "SUPP_NATION",
      "N2"."N_NAME" as "CUST_NATION",
      extract(year from "L"."L_SHIPDATE") as "L_YEAR",
      "L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT") as "VOLUME"
    from
      "SUPPLIER" "S",
      "LINEITEM" "L",
      "ORDERS" "O",
      "CUSTOMER" "C",
      "NATION" "N1",
      "NATION" "N2"
    where
      "S"."S_SUPPKEY" = "L"."L_SUPPKEY"
      and "O"."O_ORDERKEY" = "L"."L_ORDERKEY"
      and "C"."C_CUSTKEY" = "O"."O_CUSTKEY"
      and "S"."S_NATIONKEY" = "N1"."N_NATIONKEY"
      and "C"."C_NATIONKEY" = "N2"."N_NATIONKEY"
      and (
        ("N1"."N_NAME" = 'EGYPT' and "N2"."N_NAME" = 'UNITED STATES')
        or ("N1"."N_NAME" = 'UNITED STATES' and "N2"."N_NAME" = 'EGYPT')
      )
      and "L"."L_SHIPDATE" between date '1995-01-01' and date '1996-12-31'
  ) as "SHIPPING"
group by
  "SUPP_NATION",
  "CUST_NATION",
  "L_YEAR"
order by
  "SUPP_NATION",
  "CUST_NATION",
  "L_YEAR"
