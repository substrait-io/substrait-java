select
  "C"."C_CUSTKEY",
  "C"."C_NAME",
  sum("L"."L_EXTENDEDPRICE" * (1 - "L"."L_DISCOUNT")) as "REVENUE",
  "C"."C_ACCTBAL",
  "N"."N_NAME",
  "C"."C_ADDRESS",
  "C"."C_PHONE",
  "C"."C_COMMENT"
from
  "CUSTOMER" "C",
  "ORDERS" "O",
  "LINEITEM" "L",
  "NATION" "N"
where
  "C"."C_CUSTKEY" = "O"."O_CUSTKEY"
  and "L"."L_ORDERKEY" = "O"."O_ORDERKEY"
  and "O"."O_ORDERDATE" >= date '1994-03-01'
  and "O"."O_ORDERDATE" < date '1994-03-01' + interval '3 months'
  and "L"."L_RETURNFLAG" = 'R'
  and "C"."C_NATIONKEY" = "N"."N_NATIONKEY"
group by
  "C"."C_CUSTKEY",
  "C"."C_NAME",
  "C"."C_ACCTBAL",
  "C"."C_PHONE",
  "N"."N_NAME",
  "C"."C_ADDRESS",
  "C"."C_COMMENT"
order by
  "REVENUE" desc
limit 20
