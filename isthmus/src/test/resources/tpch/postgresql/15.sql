-- converted to CTE since DDL is not part of Substrait.
with "REVENUE0"("SUPPLIER_NO", "TOTAL_REVENUE") as (
  select
    "L_SUPPKEY",
    sum("L_EXTENDEDPRICE" * (1 - "L_DISCOUNT"))
  from
    "LINEITEM"
  where
    "L_SHIPDATE" >= date '1993-05-01'
    and "L_SHIPDATE" < date '1993-05-01' + interval '3 month'
  group by
    "L_SUPPKEY")

select
  "S"."S_SUPPKEY",
  "S"."S_NAME",
  "S"."S_ADDRESS",
  "S"."S_PHONE",
  "R"."TOTAL_REVENUE"
from
  "SUPPLIER" "S",
  "REVENUE0" "R"
where
  "S"."S_SUPPKEY" = "R"."SUPPLIER_NO"
  and "R"."TOTAL_REVENUE" = (
    select
      max("TOTAL_REVENUE")
    from
      "REVENUE0"
  )
order by
  "S"."S_SUPPKEY"
