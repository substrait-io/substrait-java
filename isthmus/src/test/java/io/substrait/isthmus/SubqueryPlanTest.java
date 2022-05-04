package io.substrait.isthmus;

import static io.substrait.isthmus.PlanTestBase.asString;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Plan;
import java.io.IOException;
import java.util.Arrays;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class SubqueryPlanTest {
  @Test
  public void existsCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    Plan plan =
        s.execute(
            "select l_partkey from lineitem where exists (select o_orderdate from orders where o_orderkey = l_orderkey)",
            creates);
    String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    assertTrue(planText.contains("setPredicate"));
    assertTrue(planText.contains("PREDICATE_OP_EXISTS"));
    assertTrue(planText.contains("\"stepsOut\": 1"));
  }

  @Test
  public void uniqueCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    Plan plan =
        s.execute(
            "select l_partkey from lineitem where unique (select o_orderdate from orders where o_orderkey = l_orderkey)",
            creates);
    String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    assertTrue(planText.contains("setPredicate"));
    assertTrue(planText.contains("PREDICATE_OP_UNIQUE"));
    assertTrue(planText.contains("\"stepsOut\": 1"));
  }

  @Test
  public void existsNestedCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
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
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    Plan plan = s.execute(sql, creates);
    String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    assertTrue(planText.contains("setPredicate"));
    assertTrue(planText.contains("PREDICATE_OP_EXISTS"));
    assertTrue(planText.contains("PREDICATE_OP_UNIQUE"));

    assertTrue(planText.contains("\"stepsOut\": 2"));
    assertTrue(planText.contains("\"stepsOut\": 1"));
  }

  @Test
  public void inPredicateCorrelatedSubQuery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    String sql =
        "select l_orderkey from lineitem where l_partkey in (select p_partkey from part where p_partkey = l_partkey)";
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    Plan plan = s.execute(sql, creates);
    String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    assertTrue(planText.contains("inPredicate"));
    assertTrue(planText.contains("\"stepsOut\": 1"));
  }

  @Test
  public void notInPredicateCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    String sql =
        "select l_orderkey from lineitem where l_partkey not in (select p_partkey from part where p_partkey = l_partkey)";
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    Plan plan = s.execute(sql, creates);
    String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    assertTrue(planText.contains("inPredicate"));
    assertTrue(planText.contains("\"stepsOut\": 1"));
  }

  @Test
  public void nestedScalarCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String[] values = asString("tpch/schema.sql").split(";");
    String sql = asString("subquery/nested_scalar_subquery.sql");
    var creates = Arrays.stream(values).filter(t -> !t.trim().isBlank()).toList();
    Plan plan = s.execute(sql, creates);
    String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    assertTrue(planText.contains("\"stepsOut\": 2"));
    assertTrue(planText.contains("\"stepsOut\": 1"));
    assertTrue(planText.contains("\"scalar\""));

    // System.out.println(JsonFormat.printer().includingDefaultValueFields().print(plan));
  }
}
