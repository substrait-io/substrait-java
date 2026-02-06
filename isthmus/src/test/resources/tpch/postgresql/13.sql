select
  "C_COUNT",
  count(*) as "CUSTDIST"
from
  (
    select
      "C"."C_CUSTKEY",
      count("O"."O_ORDERKEY")
    from
      "CUSTOMER" "C"
      left outer join "ORDERS" "O"
        on "C"."C_CUSTKEY" = "O"."O_CUSTKEY"
        and "O"."O_COMMENT" not like '%special%requests%'
    group by
      "C"."C_CUSTKEY"
  ) as "ORDERS" ("C_CUSTKEY", "C_COUNT")
group by
  "C_COUNT"
order by
  "CUSTDIST" desc,
  "C_COUNT" desc
