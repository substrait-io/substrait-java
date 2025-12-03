package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.isthmus.utils.SetUtils;
import io.substrait.plan.Plan;
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

class ProtoPlanConverterTest extends PlanTestBase {

  private io.substrait.proto.Plan getProtoPlan(final String query1) throws SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
    return toProto(s.convert(query1, TPCH_CATALOG));
  }

  @Test
  void aggregate() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select count(L_ORDERKEY),sum(L_ORDERKEY) from lineitem");
  }

  @Test
  void simpleSelect() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select l_orderkey,l_extendedprice from lineitem");
  }

  private static void assertAggregateInvocationDistinct(final io.substrait.proto.Plan plan) {
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
  void distinctCount() throws IOException, SqlParseException {
    final String distinctQuery = "select count(DISTINCT L_ORDERKEY) from lineitem";
    final io.substrait.proto.Plan protoPlan = getProtoPlan(distinctQuery);
    assertAggregateInvocationDistinct(protoPlan);
    assertAggregateInvocationDistinct(toProto(new ProtoPlanConverter().from(protoPlan)));
  }

  @Test
  void filter() throws IOException, SqlParseException {
    assertProtoPlanRoundrip("select L_ORDERKEY from lineitem WHERE L_ORDERKEY + 1 > 10");
  }

  @Test
  void crossJoin() throws IOException, SqlParseException {
    final int[] counter = new int[1];
    final RelCopyOnWriteVisitor<RuntimeException> crossJoinCountingVisitor =
        new RelCopyOnWriteVisitor<RuntimeException>() {
          @Override
          public Optional<Rel> visit(final Cross cross, final EmptyVisitationContext context)
              throws RuntimeException {
            counter[0]++;
            return super.visit(cross, context);
          }
        };
    final ImmutableFeatureBoard featureBoard = ImmutableFeatureBoard.builder().build();

    final String query1 =
        "select\n"
            + "  c.c_custKey,\n"
            + "  o.o_custkey\n"
            + "from\n"
            + "  \"customer\" c cross join\n"
            + "  \"orders\" o";
    final Plan plan1 = assertProtoPlanRoundrip(query1, new SqlToSubstrait(featureBoard));
    plan1
        .getRoots()
        .forEach(
            t -> t.getInput().accept(crossJoinCountingVisitor, EmptyVisitationContext.INSTANCE));
    assertEquals(1, counter[0]);

    final String query2 =
        "select\n"
            + "  c.c_custKey,\n"
            + "  o.o_custkey\n"
            + "from\n"
            + "  \"customer\" c,\n"
            + "  \"orders\" o";
    final Plan plan2 = assertProtoPlanRoundrip(query2, new SqlToSubstrait(featureBoard));
    plan2
        .getRoots()
        .forEach(
            t -> t.getInput().accept(crossJoinCountingVisitor, EmptyVisitationContext.INSTANCE));
    assertEquals(2, counter[0]);
  }

  @Test
  void joinAggSortLimit() throws IOException, SqlParseException {
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
  void setTest(final Set.SetOp op, final boolean multi) throws Exception {
    assertProtoPlanRoundrip(SetUtils.getSetQuery(op, multi));
  }

  @Test
  void existsCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_partkey from lineitem where exists (select o_orderdate from orders where o_orderkey = l_orderkey)");
  }

  @Test
  void uniqueCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_partkey from lineitem where unique (select o_orderdate from orders where o_orderkey = l_orderkey)");
  }

  @Test
  void inPredicateCorrelatedSubQuery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_orderkey from lineitem where l_partkey in (select p_partkey from part where p_partkey = l_partkey)");
  }

  @Test
  void notInPredicateCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(
        "select l_orderkey from lineitem where l_partkey not in (select p_partkey from part where p_partkey = l_partkey)");
  }

  @Test
  void existsNestedCorrelatedSubquery() throws IOException, SqlParseException {
    final String sql =
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
  void nestedScalarCorrelatedSubquery() throws IOException, SqlParseException {
    assertProtoPlanRoundrip(asString("subquery/nested_scalar_subquery_in_filter.sql"));
  }
}
