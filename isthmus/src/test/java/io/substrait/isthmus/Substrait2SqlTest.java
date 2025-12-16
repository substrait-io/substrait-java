package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.isthmus.utils.SetUtils;
import io.substrait.plan.Plan;
import io.substrait.relation.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class Substrait2SqlTest extends PlanTestBase {
  private void assertSqlRoundTripViaPojoAndProto(String inputSql) {
    Plan plan =
        assertDoesNotThrow(() -> toSubstraitPlan(inputSql, TPCH_CATALOG), "SQL to Substrait POJO");
    assertDoesNotThrow(() -> toSql(plan), "Substrait POJO to SQL");
    io.substrait.proto.Plan proto =
        assertDoesNotThrow(() -> toProto(plan), "Substrait POJO to Substrait PROTO");
    assertDoesNotThrow(() -> toSql(proto), "Substrait PROTO to SQL");
  }

  @Test
  void simpleTest() throws Exception {
    String query = "select p_size  from part where p_partkey > cast(100 as bigint)";
    assertFullRoundTrip(query);
  }

  @Test
  void simpleTest2() throws Exception {
    String query =
        "select l_partkey, l_discount from lineitem where l_orderkey > cast(100 as bigint)";
    assertFullRoundTrip(query);
  }

  @Test
  void simpleTestDateInterval() throws Exception {
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' ");
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '3' month ");
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '1' year");
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '1-3' year to month");
  }

  @Test
  void simpleTestDecimal() throws Exception {
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0 from lineitem where l_shipdate < date '1998-01-01' ");
  }

  @Test
  void simpleJoin() throws Exception {
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem left join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem right join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    assertFullRoundTrip(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem full join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
  }

  @Test
  void simpleTestAgg() throws Exception {
    assertFullRoundTrip(
        "select l_partkey, count(l_tax), COUNT(distinct l_discount) from lineitem group by l_partkey");

    // group by an expression
    assertFullRoundTrip(
        "select count(l_tax), COUNT(distinct l_discount) from lineitem group by l_partkey + l_orderkey");
  }

  @Test
  void simpleTestGroupingSets() throws Exception {
    assertFullRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate)");
    assertFullRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate), l_linestatus");

    assertFullRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate, ())");

    assertFullRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate, ()), l_linestatus");
    assertFullRoundTrip(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), (l_orderkey, L_COMMITDATE, l_linestatus), l_shipdate, ())");

    // GROUP_ID()
    assertFullRoundTrip(
        "select sum(l_discount), group_id() from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate)");
    assertFullRoundTrip(
        "select group_id(), sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate)");
    assertFullRoundTrip(
        "select group_id(), sum(l_discount), group_id() from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate)");
  }

  @Test
  void testRollup() {
    assertSqlRoundTripViaPojoAndProto(
        "select charcol from (select charcol, count(*) from (values('a')) as t(charcol) group by rollup(charcol))");
  }

  @Test
  void simpleTestAggFilter() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "select sum(l_tax) filter(WHERE l_orderkey > l_partkey) from lineitem");
    // cast is added to avoid the difference by implicit cast
    assertSqlSubstraitRelRoundTrip(
        "select sum(l_tax) filter(WHERE l_orderkey > cast(10.0 as bigint)) from lineitem");
  }

  @Test
  void simpleTestAggNoGB() throws Exception {
    assertFullRoundTrip("select count(l_tax), count(distinct l_discount) from lineitem");
  }

  @Test
  void simpleTestAgg2() throws Exception {
    assertFullRoundTrip(
        "select l_partkey, sum(l_tax), sum(distinct l_tax), avg(l_discount), avg(distinct l_discount) from lineitem group by l_partkey");
  }

  @Test
  void simpleTestAgg3() throws Exception {
    assertFullRoundTrip(
        "select l_partkey, sum(l_extendedprice * (1.0 - l_discount)) from lineitem group by l_partkey");
  }

  @ParameterizedTest
  @MethodSource("io.substrait.isthmus.utils.SetUtils#setTestConfig")
  void setTest(Set.SetOp op, boolean multi) throws Exception {
    assertFullRoundTrip(SetUtils.getSetQuery(op, multi));
  }

  @Test
  void tpch_q1_variant() throws Exception {
    // difference from tpch_q1 : 1) remove order by clause; 2) remove interval date literal
    assertFullRoundTrip(
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
  void simpleTestApproxCountDistinct() throws Exception {
    String query = "select approx_count_distinct(l_tax)  from lineitem";
    RelRoot relRoot = assertSqlSubstraitRelRoundTrip(query);
    RelNode relNode = relRoot.project();

    // Assert converted Calcite RelNode has `approx_count_distinct`
    assertInstanceOf(LogicalAggregate.class, relNode);
    LogicalAggregate aggregate = (LogicalAggregate) relNode;
    assertEquals(
        SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
        aggregate.getAggCallList().get(0).getAggregation());
  }

  @Test
  void simpleOrderByClause() throws Exception {
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
  void simpleStringOpTest() throws Exception {
    assertFullRoundTrip("select substring(l_comment, 1, 5) from lineitem");

    assertFullRoundTrip("select lower(l_comment) from lineitem");
    assertFullRoundTrip("select l_comment from lineitem where lower(l_comment) <> l_comment");

    assertFullRoundTrip("select upper(l_comment) from lineitem");
    assertFullRoundTrip("select l_comment from lineitem where upper(l_comment) <> l_comment");
  }

  @Test
  void caseWhenTest() throws Exception {
    assertFullRoundTrip(
        "select case when p_size > 100 then 'large' when p_size > 50 then 'medium' else 'small' end from part");
  }
}
