SELECT p_partkey, p_size
FROM part p
WHERE p_size <
      (SELECT sum(l_orderkey)
       FROM lineitem l
       WHERE l.l_partkey = p.p_partkey
         AND l_linenumber >
             (SELECT count(*) cnt
              FROM partsupp ps
              WHERE ps.ps_partkey = p.p_partkey
                AND   PS.ps_suppkey = l.l_suppkey))
