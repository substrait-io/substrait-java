select
  "S"."S_ACCTBAL",
  "S"."S_NAME",
  "N"."N_NAME",
  "P"."P_PARTKEY",
  "P"."P_MFGR",
  "S"."S_ADDRESS",
  "S"."S_PHONE",
  "S"."S_COMMENT"
from
  "PART" "P",
  "SUPPLIER" "S",
  "PARTSUPP" "PS",
  "NATION" "N",
  "REGION" "R"
where
  "P"."P_PARTKEY" = "PS"."PS_PARTKEY"
  and "S"."S_SUPPKEY" = "PS"."PS_SUPPKEY"
  and "P"."P_SIZE" = 41
  and "P"."P_TYPE" like '%NICKEL'
  and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
  and "N"."N_REGIONKEY" = "R"."R_REGIONKEY"
  and "R"."R_NAME" = 'EUROPE'
  and "PS"."PS_SUPPLYCOST" = (

    select
      min("PS"."PS_SUPPLYCOST")

    from
      "PARTSUPP" "PS",
      "SUPPLIER" "S",
      "NATION" "N",
      "REGION" "R"
    where
      "P"."P_PARTKEY" = "PS"."PS_PARTKEY"
      and "S"."S_SUPPKEY" = "PS"."PS_SUPPKEY"
      and "S"."S_NATIONKEY" = "N"."N_NATIONKEY"
      and "N"."N_REGIONKEY" = "R"."R_REGIONKEY"
      and "R"."R_NAME" = 'EUROPE'
  )

order by
  "S"."S_ACCTBAL" desc,
  "N"."N_NAME",
  "S"."S_NAME",
  "P"."P_PARTKEY"
limit 100
