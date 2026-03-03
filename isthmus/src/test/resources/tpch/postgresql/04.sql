select
  "O"."O_ORDERPRIORITY",
  count(*) as "ORDER_COUNT"
from
  "ORDERS" "O"

where
  "O"."O_ORDERDATE" >= date '1996-10-01'
  and "O"."O_ORDERDATE" < date '1996-10-01' + interval '3 months'
  and
  exists (
    select
      *
    from
      "LINEITEM" "L"
    where
      "L"."L_ORDERKEY" = "O"."O_ORDERKEY"
      and "L"."L_COMMITDATE" < "L"."L_RECEIPTDATE"
  )
group by
  "O"."O_ORDERPRIORITY"
order by
  "O"."O_ORDERPRIORITY"
