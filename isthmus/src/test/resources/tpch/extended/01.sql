select
    c1.c_name,
    o1.o_orderstatus,
    o1.o_totalprice
from
    customer c1,
    orders o1
where
    o1.o_custkey = c1.c_custkey
    and o1.o_totalprice > (
        select
            avg(o_totalprice)
        from
            orders o2
        where
            o2.o_totalprice < c1.c_acctbal
            and o2.o_orderpriority = c1.c_phone
            and o2.o_totalprice > (
                select
                    avg(c3.c_acctbal)
                from
                    customer c3
                where
                    c1.c_custkey = o2.o_custkey
                    and c3.c_address = o2.o_clerk
            )
    );