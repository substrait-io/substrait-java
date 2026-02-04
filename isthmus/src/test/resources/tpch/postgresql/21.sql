select
  "S"."S_NAME",
  count(*) as "NUMWAIT"
from
  "SUPPLIER" "S",
  "LINEITEM" "L1",
  "ORDERS" "O",
  "NATION" "N"
where
  "S"."S_SUPPKEY" = "L1"."L_SUPPKEY"
  and "O"."O_ORDERKEY" = "L1"."L_ORDERKEY"
  and "O"."O_ORDERSTATUS" = 'F'
  and "L1"."L_RECEIPTDATE" > "L1"."L_COMMITDATE"
  and exists (
    select
      *
    from
      "LINEITEM" "L2"
    where
      "L2"."L_ORDERKEY" = "L1"."L_ORDERKEY"
      and "L2"."L_SUPPKEY" <> "L1"."L_SUPPKEY"
  )
  and not exists (
    select
      *
    from
      "LINEITEM" "L3"
    where
      "L3"."L_ORDERKEY" = "L1"."L_ORDERKEY"
      and "L3"."L_SUPPKEY" <> "L1"."L_SUPPKEY"
      and "L3"."L_RECEIPTDATE" > "L3"."L_COMMITDATE"
  )
  and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
  and "N"."N_NAME" = 'BRAZIL'
group by
  "S"."S_NAME"
order by
  "NUMWAIT" desc,
  "S"."S_NAME"
limit 100
