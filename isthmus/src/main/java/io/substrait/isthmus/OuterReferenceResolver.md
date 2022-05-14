### How fieldAccessDepthMap in OuterReferenceResolver is computed?

```
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


          Filter     ---  $coor0
          /      \ condition
         /  p_size <  RexSubquery
       Scan(P)            |
                          |
                         Agg
                          |
                      Project
                          |
                        Filter   --- $coor2
                        /      \
                       /        \
                Scan (L)         \
                                and 
                                /   \  
                              /       \ 
         l_partkey=$corr0.p_partkey   l_linenumber > RexSubQuery
                                        |
                                        Agg
                                        |
                                      Project
                                        |
                                      Filter
                                       /   \
                                      /      \
                                 Scan(PS)      and 
                                            /    \
                                          /       \
                  ps_partkey = $corr0.p_partkey   ps_suppkey = $corr2.l_suppkey

Input: 
    After SqlToRelConverter is called, LogicalFilter would be created with variablesSet as shown in the above.
Steps:
    1) When a Filter with variablesSet is visited, initialize `nestedDepth` with pair of <CorrelationID, 0>
    2) When a RexSubquery is visited, all entries in `nestedDepth` will have depth to be increased by 1. 
    3) When RexFieldAccess with reference to a CorrelationID is visited, its `steps_out`  will be depth in `nestedDepth`.
    4) When exit from RexSubQuery visit, decrease all entries in `nestedDepth` by 1

fieldAccessDepthMap:
    l_partkey=$corr0.p_partkey:    $corr0.p_partkey -> 1
    ps_partkey = $corr0.p_partkey: $corr0.p_partkey -> 2
    ps_suppkey = $corr2.l_suppkey: $corr2.l_suppkey -> 1

```