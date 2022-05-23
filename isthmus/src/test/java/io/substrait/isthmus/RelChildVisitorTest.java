package io.substrait.isthmus;

import static io.substrait.isthmus.PlanTestBase.asString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.plan.Plan;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.AggregateFunction;
import io.substrait.relation.Aggregate;
import io.substrait.relation.NamedScan;
import io.substrait.relation.RelChildVisitor;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class RelChildVisitorTest {
  private Plan buildPlanFromQuery(String query) throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    io.substrait.proto.Plan protoPlan1 = s.execute(query, creates);
    return new ProtoPlanConverter().from(protoPlan1);
  }

  @Test
  public void hasTableReference() throws IOException, SqlParseException {
    Plan plan =
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
    assertTrue(hasTableReference(plan, "PARTSUPP"));
    assertTrue(hasTableReference(plan, "LINEITEM"));
    assertTrue(hasTableReference(plan, "PART"));
    assertFalse(hasTableReference(plan, "FOO"));
  }

  @Test
  public void countCountDistincts() throws IOException, SqlParseException {
    Plan plan =
        buildPlanFromQuery(
            "select\n"
                + "  count(distinct l.l_orderkey),\n"
                + "  count(distinct l.l_orderkey) + 1,\n"
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
                + "limit 10");
    CountCountDistinct visitor = new CountCountDistinct();
    plan.getRoots().stream().forEach(r -> r.getInput().accept(visitor));
    assertEquals(2, visitor.getCountDistincts());
  }

  private boolean hasTableReference(Plan plan, String name) {
    HasTableReference visitor = new HasTableReference(Arrays.asList(name));
    plan.getRoots().stream().forEach(r -> r.getInput().accept(visitor));
    return (visitor.hasTableReference());
  }

  private static class HasTableReference extends RelChildVisitor {
    private boolean hasTableReference;
    private final List<String> tableName;

    public HasTableReference(List<String> tableName) {
      this.tableName = tableName;
    }

    public boolean hasTableReference() {
      return hasTableReference;
    }

    @Override
    public NamedScan visit(NamedScan namedScan) {
      this.hasTableReference |= namedScan.getNames().equals(tableName);
      return namedScan;
    }
  }

  private static class CountCountDistinct extends RelChildVisitor {
    private int countDistincts;

    public int getCountDistincts() {
      return countDistincts;
    }

    @Override
    public Aggregate visit(Aggregate aggregate) {
      super.visit(aggregate);
      for (Aggregate.Measure m : aggregate.getMeasures()) {
        if (m.getFunction().invocation()
            == AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT) {
          countDistincts++;
        }
      }
      return aggregate;
    }
  }
}
