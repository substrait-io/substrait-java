package io.substrait.isthmus;

import static io.substrait.isthmus.SqlToSubstrait.EXTENSION_COLLECTION;

import io.substrait.relation.Rel;
import java.util.Arrays;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class Substrait2SqlTest extends PlanTestBase {
  @Test
  public void simpleTest() throws Exception {
    String query = "select p_size  from part where p_partkey > cast(100 as bigint)";
    test(query);
  }

  @Test
  public void simpleTest2() throws Exception {
    String query =
        "select l_partkey, l_discount from lineitem where l_orderkey > cast(100 as bigint)";
    test(query);
  }

  @Test
  public void simpleTestDateInterval() throws Exception {
    test(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' ");
    test(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '3' month ");
    test(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '1' year");
    test(
        "select l_partkey + l_orderkey, l_shipdate from lineitem where l_shipdate < date '1998-01-01' + interval '1-3' year to month");
  }

  @Test
  public void simpleTestDecimal() throws Exception {
    test(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0 from lineitem where l_shipdate < date '1998-01-01' ");
  }

  @Test
  public void simpleJoin() throws Exception {
    test(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    test(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem left join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    test(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem right join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
    test(
        "select l_partkey + l_orderkey, l_extendedprice * 0.1 + 100.0, o_orderkey from lineitem full join orders on l_orderkey = o_orderkey where l_shipdate < date '1998-01-01' ");
  }

  @Test
  public void simpleTestAgg() throws Exception {
    test(
        "select l_partkey, count(l_tax), COUNT(distinct l_discount) from lineitem group by l_partkey");

    // group by an expression
    test(
        "select count(l_tax), COUNT(distinct l_discount) from lineitem group by l_partkey + l_orderkey");
  }

  @Test
  public void simpleTestGroupingSets() throws Exception {
    test(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate)");
    test(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate), l_linestatus");

    test(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate, ())");

    test(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), l_shipdate, ()), l_linestatus");
    test(
        "select sum(l_discount) from lineitem group by grouping sets ((l_orderkey, L_COMMITDATE), (l_orderkey, L_COMMITDATE, l_linestatus), l_shipdate, ())");
  }

  @Test
  public void simpleTestAggFilter() throws Exception {
    test("select sum(l_tax) filter(WHERE l_orderkey > l_partkey) from lineitem");
    // cast is added to avoid the difference by implicit cast
    test("select sum(l_tax) filter(WHERE l_orderkey > cast(10.0 as bigint)) from lineitem");
  }

  @Test
  public void simpleTestAggNoGB() throws Exception {
    test("select count(l_tax), count(distinct l_discount) from lineitem");
  }

  @Test
  public void simpleTestAgg2() throws Exception {
    test(
        "select l_partkey, sum(l_tax), sum(distinct l_tax), avg(l_discount), avg(distinct l_discount) from lineitem group by l_partkey");
  }

  @Test
  public void simpleTestAgg3() throws Exception {
    test(
        "select l_partkey, sum(l_extendedprice * (1.0 - l_discount)) from lineitem group by l_partkey");
  }

  @Test
  public void tpch_q1_variant() throws Exception {
    // difference from tpch_q1 : 1) remove order by clause; 2) remove interval date literal
    test(
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

  private void test(String query) throws Exception {
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();

    // 1. sql -> substrait rel
    SqlToSubstrait s = new SqlToSubstrait();
    RelRoot relRoot = s.sqlToRelNode(query, creates);
    Rel pojoRel = SubstraitRelVisitor.convert(relRoot, EXTENSION_COLLECTION);

    // 2. substrait rel -> Calcite Rel
    RelNode relnodeRoot = new SubstraitToSql().substraitRelToCalciteRel(pojoRel, creates);

    // 3. Calcite Rel -> substrait rel
    Rel pojoRel2 =
        SubstraitRelVisitor.convert(RelRoot.of(relnodeRoot, SqlKind.SELECT), EXTENSION_COLLECTION);

    Assertions.assertEquals(pojoRel, pojoRel2);

    // 4. Calcite Rel -> sql
    System.out.println(SubstraitToSql.toSql(relnodeRoot));
  }
}
