select
  "S"."S_NAME",
  "S"."S_ADDRESS"
from
  "SUPPLIER" "S",
  "NATION" "N"
where
  "S"."S_SUPPKEY" in (
    select
      "PS"."PS_SUPPKEY"
    from
      "PARTSUPP" "PS"
    where
      "PS"."PS_PARTKEY" in (
        select
          "P"."P_PARTKEY"
        from
          "PART" "P"
        where
          "P"."P_NAME" like 'antique%'
      )
      and "PS"."PS_AVAILQTY" > (
        select
          0.5 * sum("L"."L_QUANTITY")
        from
          "LINEITEM" "L"
        where
          "L"."L_PARTKEY" = "PS"."PS_PARTKEY"
          and "L"."L_SUPPKEY" = "PS"."PS_SUPPKEY"
          and "L"."L_SHIPDATE" >= date '1993-01-01'
          and "L"."L_SHIPDATE" < date '1993-01-01' + interval '1 year'
      )
  )
  and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
  and "N"."N_NAME" = 'KENYA'
order by
  "S"."S_NAME"
