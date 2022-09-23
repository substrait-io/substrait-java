package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.isthmus.utils.SetUtils;
import io.substrait.relation.Set;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class Substrait2SqlTest extends PlanTestBase {

  @Test
  public void simpleTest() throws Exception {
    String query = "select p_size  from part where p_partkey > cast(100 as bigint)";
    assertSqlSubstraitRelRoundTrip(query);
  }

  @Test
  public void simpleTest2() throws Exception {
    String query =
        "select l_partkey, l_discount from lineitem where l_orderkey > cast(100 as bigint)";
    assertSqlSubstraitRelRoundTrip(query);
  }

  @Test
  public void simpleTestDateInterval() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' ");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '3' month ");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '1' year");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '1-3' year to month");
  }

  @Test
  public void simpleTestDecimal() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0 from lineitem where l_shipdate < date '1998-01-01' ");
  }

  @Test
  public void simpleJoin() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem left join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem right join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem full join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
  }

  @Test
  public void simpleTestAgg() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey, count(l_tax), COUNT(distinct l_discount) from lineitem group by l_partkey");

    // group by an expression
    assertSqlSubstraitRelRoundTrip(
        "select count(l_tax), COUNT(distinct l_discount) from lineitem group by l_partkey + l_orderkey");
  }

  @Test
  public void simpleTestGroupingSets() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate)");
    assertSqlSubstraitRelRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate), l_linestatus");

    assertSqlSubstraitRelRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate, ())");

    assertSqlSubstraitRelRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate, ()), l_linestatus");
    assertSqlSubstraitRelRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), (l_orderkey, L_COMMITDATE, l_linestatus), l_shipdate, ())");
  }

  @Test
  public void simpleTestAggFilter() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select sum(l_tax) filter(WHERE l_orderkey > l_partkey) from lineitem");
    // cast is added to avoid the difference by implicit cast
    assertSqlSubstraitRelRoundTrip(
        "select sum(l_tax) filter(WHERE l_orderkey > cast(10.0 as bigint)) from lineitem");
  }

  @Test
  public void simpleTestAggNoGB() throws Exception {
    assertSqlSubstraitRelRoundTrip("select count(l_tax), count(distinct l_discount) from lineitem");
  }

  @Test
  public void simpleTestAgg2() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey, sum(l_tax), sum(distinct l_tax), avg(l_discount), avg(distinct l_discount) from lineitem group by l_partkey");
  }

  @Test
  public void simpleTestAgg3() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey, sum(l_extendedprice * (1.0 - l_discount)) from lineitem group by l_partkey");
  }

  @ParameterizedTest
  @MethodSource("io.substrait.isthmus.utils.SetUtils#setTestConfig")
  public void setTest(Set.SetOp op, boolean multi) throws Exception {
    assertSqlSubstraitRelRoundTrip(SetUtils.getSetQuery(op, multi));
  }

  @Test
  public void tpch_q1_variant() throws Exception {
    // difference from tpch_q1 : 1) remove order by clause; 2) remove interval date literal
    assertSqlSubstraitRelRoundTrip(
        "select\n"
            + "  l_returnflag,\n"
            + "  l_linestatus,\n"
            + "  sum(l_quantity) as sum_qty,\n"
            + "  sum(l_extendedprice) as sum_base_price,\n"
            + "  sum(l_extendedprice * (1.0 - l_discount)) as sum_disc_price,\n"
            + "  sum(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) as sum_charge,\n"
            + "  avg(l_quantity) as avg_qty,\n"
            + "  avg(l_extendedprice) as avg_price,\n"
            + "  avg(l_discount) as avg_disc,\n"
            + "  count(*) as count_order\n"
            + "from\n"
            + "  lineitem\n"
            + "where\n"
            + "  l_shipdate <= date '1998-12-01' \n"
            + "group by\n"
            + "  l_returnflag,\n"
            + "  l_linestatus\n");
  }

  @Test
  public void simpleTestApproxCountDistinct() throws Exception {
    String query = "select approx_count_distinct(l_tax)  from lineitem";
    List<RelNode> relNodeList = assertSqlSubstraitRelRoundTrip(query);

    // Assert converted Calcite RelNode has `approx_count_distinct`
    RelNode relNode = relNodeList.get(0);
    assertTrue(relNode instanceof LogicalAggregate);
    LogicalAggregate aggregate = (LogicalAggregate) relNode;
    assertTrue(
        aggregate.getAggCallList().get(0).getAggregation()
            == SqlStdOperatorTable.APPROX_COUNT_DISTINCT);
  }

  @Test
  public void simpleOrderByClause() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey from lineitem where l_shipdate < date '1998-01-01' order by l_shipdate, l_discount");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey from lineitem where l_shipdate < date '1998-01-01' order by l_shipdate asc, l_discount desc");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey from lineitem where l_shipdate < date '1998-01-01' order by l_shipdate asc, l_discount desc limit 100 offset 1000");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey from lineitem where l_shipdate < date '1998-01-01' order by l_shipdate asc, l_discount desc limit 100");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey from lineitem where l_shipdate < date '1998-01-01' limit 100");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey from lineitem where l_shipdate < date '1998-01-01' order by l_shipdate asc, l_discount desc nulls first");
    assertSqlSubstraitRelRoundTrip(
        "select l_partkey from lineitem where l_shipdate < date '1998-01-01' order by l_shipdate asc, l_discount desc nulls last");
  }

  @Test
  public void simpleTestSingleOrList() throws Exception {
    assertSqlSubstraitRelRoundTrip("select l_partkey from lineitem where L_PARTKEY in (1,2,3)");
    assertSqlSubstraitRelRoundTrip("select l_partkey from lineitem where L_PARTKEY = 1 or L_PARTKEY = 2 or L_PARTKEY = 3 ");
  }
}
