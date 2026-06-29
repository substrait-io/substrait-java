select
  "PS"."PS_PARTKEY",
  sum("PS"."PS_SUPPLYCOST" * "PS"."PS_AVAILQTY") as "VALUE"
from
  "PARTSUPP" "PS",
  "SUPPLIER" "S",
  "NATION" "N"
where
  "PS"."PS_SUPPKEY" = "S"."S_SUPPKEY"
  and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
  and "N"."N_NAME" = 'JAPAN'
group by
  "PS"."PS_PARTKEY" having
    sum("PS"."PS_SUPPLYCOST" * "PS"."PS_AVAILQTY") > (
      select
        sum("PS"."PS_SUPPLYCOST" * "PS"."PS_AVAILQTY") * 0.0001000000
      from
        "PARTSUPP" "PS",
        "SUPPLIER" "S",
        "NATION" "N"
      where
        "PS"."PS_SUPPKEY" = "S"."S_SUPPKEY"
        and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
        and "N"."N_NAME" = 'JAPAN'
    )
order by
  "VALUE" desc
