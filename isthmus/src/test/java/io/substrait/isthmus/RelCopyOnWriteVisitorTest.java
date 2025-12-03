package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitSqlDialect;
import io.substrait.plan.Plan;
import io.substrait.relation.Aggregate;
import io.substrait.relation.CopyOnWriteUtils;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Rel;
import io.substrait.relation.RelCopyOnWriteVisitor;
import io.substrait.util.EmptyVisitationContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class RelCopyOnWriteVisitorTest extends PlanTestBase {

  public static SimpleExtension.FunctionAnchor APPROX_COUNT_DISTINCT =
      SimpleExtension.FunctionAnchor.of(
          "extension:io.substrait:functions_aggregate_approx", "approx_count_distinct:any");
  public static SimpleExtension.FunctionAnchor COUNT =
      SimpleExtension.FunctionAnchor.of(
          "extension:io.substrait:functions_aggregate_generic", "count:any");

  private static final String COUNT_DISTINCT_SUBBQUERY =
      "select\n"
          + "  count(distinct l.l_orderkey),\n"
          + " count(distinct l.l_orderkey) + 1,\n"
          + "  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue,\n"
          + "  o.o_orderdate,\n"
          + "  count(distinct o.o_shippriority)\n"
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
          + "having\n"
          + "  count(distinct o.o_shippriority) > 2\n"
          + "order by\n"
          + "  revenue desc,\n"
          + "  o.o_orderdate\n"
          + "limit 10";

  private static final String UNION_DISTINCT_COUNT_QUERY =
      "select\n"
          + "  count(distinct l.l_orderkey) as cnt\n"
          + "from\n"
          + "  \"lineitem\" l\n"
          + "union\n"
          + "select\n"
          + "  count(distinct o.o_orderkey) as cnt\n"
          + "from\n"
          + "  \"orders\" o\n";

  private Plan buildPlanFromQuery(final String query) throws IOException, SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
    return s.convert(query, TPCH_CATALOG);
  }

  @Test
  void hasTableReference() throws IOException, SqlParseException {
    final Plan plan =
        buildPlanFromQuery(
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
                + "          AND   PS.ps_suppkey = l.l_suppkey))");
    final HasTableReference action = new HasTableReference();
    assertTrue(action.hasTableReference(plan, "PARTSUPP"));
    assertTrue(action.hasTableReference(plan, "LINEITEM"));
    assertTrue(action.hasTableReference(plan, "PART"));
    assertFalse(action.hasTableReference(plan, "FOO"));
  }

  @Test
  void countCountDistincts() throws IOException, SqlParseException {
    final Plan plan = buildPlanFromQuery(COUNT_DISTINCT_SUBBQUERY);
    assertEquals(2, new CountCountDistinct().getCountDistincts(plan));
  }

  @Test
  void replaceCountDistincts() throws IOException, SqlParseException {
    final Plan oldPlan = buildPlanFromQuery(COUNT_DISTINCT_SUBBQUERY);
    assertEquals(2, new CountCountDistinct().getCountDistincts(oldPlan));
    assertEquals(0, new CountApproxCountDistinct().getApproxCountDistincts(oldPlan));
    final ReplaceCountDistinctWithApprox action = new ReplaceCountDistinctWithApprox();
    final Plan newPlan = action.modify(oldPlan).orElse(oldPlan);
    assertEquals(2, new CountApproxCountDistinct().getApproxCountDistincts(newPlan));
    assertEquals(0, new CountCountDistinct().getCountDistincts(newPlan));
    assertPlanRoundtrip(newPlan);
  }

  @Test
  void approximateCountDistinct() throws IOException, SqlParseException {
    final Plan oldPlan =
        buildPlanFromQuery(
            "select count(distinct l_discount), count(distinct l_tax) from lineitem");
    assertEquals(2, new CountCountDistinct().getCountDistincts(oldPlan));
    assertEquals(0, new CountApproxCountDistinct().getApproxCountDistincts(oldPlan));
    final ReplaceCountDistinctWithApprox action = new ReplaceCountDistinctWithApprox();
    final Plan newPlan = action.modify(oldPlan).orElse(oldPlan);
    assertEquals(2, new CountApproxCountDistinct().getApproxCountDistincts(newPlan));
    assertEquals(0, new CountCountDistinct().getCountDistincts(newPlan));
    assertPlanRoundtrip(newPlan);

    // convert newPlan back to sql
    final Rel pojoRel = newPlan.getRoots().get(0).getInput();
    final RelNode relnodeRoot =
        new SubstraitToSql().substraitRelToCalciteRel(pojoRel, TPCH_CATALOG);
    final String newSql = SubstraitSqlDialect.toSql(relnodeRoot).getSql();
    assertTrue(newSql.toUpperCase().contains("APPROX_COUNT_DISTINCT"));
  }

  @Test
  void countCountDistinctsUnion() throws IOException, SqlParseException {
    final Plan plan = buildPlanFromQuery(UNION_DISTINCT_COUNT_QUERY);
    assertEquals(2, new CountCountDistinct().getCountDistincts(plan));
  }

  @Test
  void replaceCountDistinctsInUnion() throws IOException, SqlParseException {
    final Plan oldPlan = buildPlanFromQuery(UNION_DISTINCT_COUNT_QUERY);
    assertEquals(2, new CountCountDistinct().getCountDistincts(oldPlan));
    assertEquals(0, new CountApproxCountDistinct().getApproxCountDistincts(oldPlan));
    final ReplaceCountDistinctWithApprox action = new ReplaceCountDistinctWithApprox();
    final Plan newPlan = action.modify(oldPlan).orElse(oldPlan);
    assertEquals(2, new CountApproxCountDistinct().getApproxCountDistincts(newPlan));
    assertEquals(0, new CountCountDistinct().getCountDistincts(newPlan));
    assertPlanRoundtrip(newPlan);
  }

  private static class HasTableReference {
    public boolean hasTableReference(final Plan plan, final String name) {
      final HasTableReferenceVisitor visitor = new HasTableReferenceVisitor(Arrays.asList(name));
      plan.getRoots().stream()
          .forEach(r -> r.getInput().accept(visitor, EmptyVisitationContext.INSTANCE));
      return (visitor.hasTableReference());
    }

    private class HasTableReferenceVisitor extends RelCopyOnWriteVisitor<RuntimeException> {
      private boolean hasTableReference;
      private final List<String> tableName;

      public HasTableReferenceVisitor(final List<String> tableName) {
        this.tableName = tableName;
      }

      public boolean hasTableReference() {
        return hasTableReference;
      }

      @Override
      public Optional<Rel> visit(final NamedScan namedScan, final EmptyVisitationContext context) {
        this.hasTableReference |= namedScan.getNames().equals(tableName);
        return super.visit(namedScan, context);
      }
    }
  }

  private static class CountCountDistinct {

    public int getCountDistincts(final Plan plan) {
      final CountCountDistinctVisitor visitor = new CountCountDistinctVisitor();
      plan.getRoots().stream()
          .forEach(r -> r.getInput().accept(visitor, EmptyVisitationContext.INSTANCE));
      return visitor.getCountDistincts();
    }

    private static class CountCountDistinctVisitor extends RelCopyOnWriteVisitor<RuntimeException> {
      private int countDistincts;

      public int getCountDistincts() {
        return countDistincts;
      }

      @Override
      public Optional<Rel> visit(final Aggregate aggregate, final EmptyVisitationContext context) {
        countDistincts +=
            aggregate.getMeasures().stream()
                .filter(
                    m ->
                        m.getFunction().declaration().getAnchor().equals(COUNT)
                            && m.getFunction()
                                .invocation()
                                .equals(Expression.AggregationInvocation.DISTINCT))
                .count();
        return super.visit(aggregate, context);
      }
    }
  }

  private static class CountApproxCountDistinct {

    public int getApproxCountDistincts(final Plan plan) {
      final CountCountDistinctVisitor visitor = new CountCountDistinctVisitor();
      plan.getRoots().stream()
          .forEach(r -> r.getInput().accept(visitor, EmptyVisitationContext.INSTANCE));
      return visitor.getApproxCountDistincts();
    }

    private static class CountCountDistinctVisitor extends RelCopyOnWriteVisitor<RuntimeException> {
      private int aproxCountDistincts;

      public int getApproxCountDistincts() {
        return aproxCountDistincts;
      }

      @Override
      public Optional<Rel> visit(final Aggregate aggregate, final EmptyVisitationContext context) {
        aproxCountDistincts +=
            aggregate.getMeasures().stream()
                .filter(
                    m -> m.getFunction().declaration().getAnchor().equals(APPROX_COUNT_DISTINCT))
                .count();
        return super.visit(aggregate, context);
      }
    }
  }

  private static class ReplaceCountDistinctWithApprox {
    private final ReplaceCountDistinctWithApproxVisitor visitor;

    public ReplaceCountDistinctWithApprox() {
      visitor =
          new ReplaceCountDistinctWithApproxVisitor(DefaultExtensionCatalog.DEFAULT_COLLECTION);
    }

    public Optional<Plan> modify(final Plan plan) {
      return CopyOnWriteUtils.<Plan.Root, EmptyVisitationContext, RuntimeException>transformList(
              plan.getRoots(),
              null,
              (t, c) ->
                  t.getInput()
                      .accept(visitor, c)
                      .map(u -> Plan.Root.builder().from(t).input(u).build()))
          .map(t -> Plan.builder().from(plan).roots(t).build());
    }

    private static class ReplaceCountDistinctWithApproxVisitor
        extends RelCopyOnWriteVisitor<RuntimeException> {

      private final SimpleExtension.AggregateFunctionVariant approxFunc;

      public ReplaceCountDistinctWithApproxVisitor(
          final SimpleExtension.ExtensionCollection extensionCollection) {
        this.approxFunc =
            Objects.requireNonNull(extensionCollection.getAggregateFunction(APPROX_COUNT_DISTINCT));
      }

      @Override
      public Optional<Rel> visit(final Aggregate aggregate, final EmptyVisitationContext context) {
        return CopyOnWriteUtils
            .<Aggregate.Measure, EmptyVisitationContext, RuntimeException>transformList(
                aggregate.getMeasures(),
                context,
                (m, c) -> {
                  if (m.getFunction().invocation().equals(Expression.AggregationInvocation.DISTINCT)
                      && m.getFunction().declaration().getAnchor().equals(COUNT)) {
                    return Optional.of(
                        Aggregate.Measure.builder()
                            .from(m)
                            .function(
                                AggregateFunctionInvocation.builder()
                                    .from(m.getFunction())
                                    .declaration(approxFunc)
                                    .invocation(Expression.AggregationInvocation.ALL)
                                    .build())
                            .build());
                  }
                  return Optional.empty();
                })
            .map(t -> Aggregate.builder().from(aggregate).measures(t).build());
      }
    }
  }
}
