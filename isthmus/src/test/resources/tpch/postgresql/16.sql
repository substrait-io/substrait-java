select
  "P"."P_BRAND",
  "P"."P_TYPE",
  "P"."P_SIZE",
  count(distinct "PS"."PS_SUPPKEY") as "SUPPLIER_CNT"
from
  "PARTSUPP" "PS",
  "PART" "P"
where
  "P"."P_PARTKEY" = "PS"."PS_PARTKEY"
  and "P"."P_BRAND" <> 'Brand#21'
  and "P"."P_TYPE" not like 'MEDIUM PLATED%'
  and "P"."P_SIZE" in (38, 2, 8, 31, 44, 5, 14, 24)
  and "PS"."PS_SUPPKEY" not in (
    select
      "S"."S_SUPPKEY"
    from
      "SUPPLIER" "S"
    where
      "S"."S_COMMENT" like '%Customer%Complaints%'
  )
group by
  "P"."P_BRAND",
  "P"."P_TYPE",
  "P"."P_SIZE"
order by
  "SUPPLIER_CNT" desc,
  "P"."P_BRAND",
  "P"."P_TYPE",
  "P"."P_SIZE"
