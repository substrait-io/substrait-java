package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.isthmus.utils.SetUtils;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.AggregateFunction;
import io.substrait.relation.Cross;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.relation.Set;
import io.substrait.util.EmptyVisitationContext;
import java.io.IOException;
import java.util.Optional;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ProtoPlanConverterTest extends PlanTestBase {

  private io.substrait.proto.Plan getProtoPlan(String query1) throws SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    return s.execute(query1, TPCH_CATALOG);
  }

  @Test
  public void aggregate() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select count(L_ORDERKEY),sum(L_ORDERKEY) from lineitem");
  }

  @Test
  public void simpleSelect() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select l_orderkey,l_extendedprice from lineitem");
  }

  private static void assertAggregateInvocationDistinct(io.substrait.proto.Plan plan) {
    assertEquals(
        AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT,
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getAggregate()
            .getMeasuresList()
            .get(0)
            .getMeasure()
            .getInvocation());
  }

  @Test
  public void distinctCount() throws IOException, SqlParseException {
    String distinctQuery = "select count(DISTINCT L_ORDERKEY) from lineitem";
    io.substrait.proto.Plan protoPlan = getProtoPlan(distinctQuery);
    assertAggregateInvocationDistinct(protoPlan);
    assertAggregateInvocationDistinct(
        new PlanProtoConverter().toProto(new ProtoPlanConverter().from(protoPlan)));
  }

  @Test
  public void filter() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select L_ORDERKEY from lineitem WHERE L_ORDERKEY + 1 > 10");
  }

  @Test
  public void crossJoin() throws IOException, SqlParseException {
    int[] counter = new int[1];
    var crossJoinCountingVisitor =
        new RelCopyOnWriteVisitor<RuntimeException>() {
          @Override
          public Optional<Rel> visit(Cross cross, EmptyVisitationContext context)
              throws RuntimeException {
            counter[0]++;
            return super.visit(cross, context);
          }
        };
    var featureBoard = ImmutableFeatureBoard.builder().build();

    String query1 =
        "select\n"
            + "  c.c_custKey,\n"
            + "  o.o_custkey\n"
            + "from\n"
            + "  \"customer\" c cross join\n"
            + "  \"orders\" o";
    Plan plan1 = assertProtoPlanRoundrip(query1, new SqlToSubstrait(featureBoard));
    plan1
        .getRoots()
        .forEach(
            t -> t.getInput().accept(crossJoinCountingVisitor, EmptyVisitationContext.INSTANCE));
    assertEquals(1, counter[0]);

    String query2 =
        "select\n"
            + "  c.c_custKey,\n"
            + "  o.o_custkey\n"
            + "from\n"
            + "  \"customer\" c,\n"
            + "  \"orders\" o";
    Plan plan2 = assertProtoPlanRoundrip(query2, new SqlToSubstrait(featureBoard));
    plan2
        .getRoots()
        .forEach(
            t -> t.getInput().accept(crossJoinCountingVisitor, EmptyVisitationContext.INSTANCE));
    assertEquals(2, counter[0]);
  }

  @Test
  public void joinAggSortLimit() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select\n"
            + "  l.l_orderkey,\n"
            + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
            + "  o.o_orderdate,\n"
            + "  o.o_shippriority\n"
            + "\n"
            + "from\n"
            + "  \"customer\" c,\n"
            + "  \"orders\" o,\n"
            + "  \"lineitem\" l\n"
            + "\n"
            + "where\n"
            + "  c.c_mktsegment = 'HOUSEHOLD'\n"
            + "  and c.c_custkey = o.o_custkey\n"
            + "  and l.l_orderkey = o.o_orderkey\n"
            + "  and o.o_orderdate < date '1995-03-25'\n"
            + "  and l.l_shipdate > date '1995-03-25'\n"
            + "\n"
            + "group by\n"
            + "  l.l_orderkey,\n"
            + "  o.o_orderdate,\n"
            + "  o.o_shippriority\n"
            + "order by\n"
            + "  revenue desc,\n"
            + "  o.o_orderdate\n"
            + "limit 10");
  }

  @ParameterizedTest
  @MethodSource("io.substrait.isthmus.utils.SetUtils#setTestConfig")
  public void setTest(Set.SetOp op, boolean multi) throws Exception {
    assertProtoPlanRoundrip(SetUtils.getSetQuery(op, multi));
  }

  @Test
  public void existsCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_partkey from lineitem where exists (select o_orderdate from orders where o_orderkey = l_orderkey)");
  }

  @Test
  public void uniqueCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_partkey from lineitem where unique (select o_orderdate from orders where o_orderkey = l_orderkey)");
  }

  @Test
  public void inPredicateCorrelatedSubQuery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_orderkey from lineitem where l_partkey in (select p_partkey from part where p_partkey = l_partkey)");
  }

  @Test
  public void notInPredicateCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_orderkey from lineitem where l_partkey not in (select p_partkey from part where p_partkey = l_partkey)");
  }

  @Test
  public void existsNestedCorrelatedSubquery() throws IOException, SqlParseException {
    String sql =
        "SELECT p_partkey\n"
            + "FROM part p\n"
            + "WHERE EXISTS\n"
            + "    (SELECT *\n"
            + "     FROM lineitem l\n"
            + "     WHERE l.l_partkey = p.p_partkey\n"
            + "       AND UNIQUE\n"
            + "         (SELECT *\n"
            + "          FROM partsupp ps\n"
            + "          WHERE ps.ps_partkey = p.p_partkey\n"
            + "          AND   PS.ps_suppkey = l.l_suppkey))";
    assertProtoPlanRoundrip(sql);
  }

  @Test
  public void nestedScalarCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(asString("subquery/nested_scalar_subquery_in_filter.sql"));
  }
}
