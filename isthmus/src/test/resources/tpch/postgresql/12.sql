select
  "L"."L_SHIPMODE",
  sum(case
    when "O"."O_ORDERPRIORITY" = '1-URGENT'
      or "O"."O_ORDERPRIORITY" = '2-HIGH'
      then 1
    else 0
  end) as "HIGH_LINE_COUNT",
  sum(case
    when "O"."O_ORDERPRIORITY" <> '1-URGENT'
      and "O"."O_ORDERPRIORITY" <> '2-HIGH'
      then 1
    else 0
  end) as "LOW_LINE_COUNT"
from
  "ORDERS" "O",
  "LINEITEM" "L"
where
  "O"."O_ORDERKEY" = "L"."L_ORDERKEY"
  and "L"."L_SHIPMODE" in ('TRUCK', 'REG AIR')
  and "L"."L_COMMITDATE" < "L"."L_RECEIPTDATE"
  and "L"."L_SHIPDATE" < "L"."L_COMMITDATE"
  and "L"."L_RECEIPTDATE" >= date '1994-01-01'
  and "L"."L_RECEIPTDATE" < date '1994-01-01' + interval '1 year'
group by
  "L"."L_SHIPMODE"
order by
  "L"."L_SHIPMODE"
