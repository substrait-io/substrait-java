select
  "CNTRYCODE",
  count(*) as "NUMCUST",
  sum("C_ACCTBAL") as "TOTACCTBAL"
from
  (
    select
      substring("C_PHONE" from 1 for 2) as "CNTRYCODE",
      "C_ACCTBAL"
    from
      "CUSTOMER" "C"
    where
      substring("C_PHONE" from 1 for 2) in
        ('24', '31', '11', '16', '21', '20', '34')
      and "C_ACCTBAL" > (
        select
          avg("C_ACCTBAL")
        from
          "CUSTOMER"
        where
          "C_ACCTBAL" > 0.00
          and substring("C_PHONE" from 1 for 2) in
            ('24', '31', '11', '16', '21', '20', '34')
      )
      and not exists (
        select
          *
        from
          "ORDERS" "O"
        where
          "O"."O_CUSTKEY" = "C"."C_CUSTKEY"
      )
  ) as "CUSTSALE"
group by
  "CNTRYCODE"
order by
  "CNTRYCODE"
