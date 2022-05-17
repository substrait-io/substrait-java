SELECT p_partkey, (SELECT sum(l_orderkey)
                   FROM lineitem l
                   WHERE l.l_partkey = p.p_partkey
                )
FROM part p