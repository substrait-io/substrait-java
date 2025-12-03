package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Subquery.SetPredicate.PredicateOp;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import java.io.IOException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

class SubqueryPlanTest extends PlanTestBase {
  // TODO: Add a roundtrip test once the ProtoRelConverter is committed and updated to support
  // subqueries

  @Test
  void existsCorrelatedSubquery() throws SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
    final Plan plan =
        toProto(
            s.convert(
                "select l_partkey from lineitem where exists (select o_orderdate from orders where o_orderkey = l_orderkey)",
                TPCH_CATALOG));

    final Expression.Subquery subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getSubquery();

    assertTrue(subquery.hasSetPredicate());
    assertSame(PredicateOp.PREDICATE_OP_EXISTS, subquery.getSetPredicate().getPredicateOp());

    final FilterRel setPredicateFilter =
        subquery
            .getSetPredicate()
            .getTuples()
            .getFilter(); // exits (select ... from orders where o_orderkey = l_orderkey)

    final Expression.FieldReference correlatedCol =
        setPredicateFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_orderkey

    assertEquals(0, correlatedCol.getDirectReference().getStructField().getField());
    assertEquals(1, correlatedCol.getOuterReference().getStepsOut());
  }

  @Test
  void uniqueCorrelatedSubquery() throws IOException, SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
    final Plan plan =
        toProto(
            s.convert(
                "select l_partkey from lineitem where unique (select o_orderdate from orders where o_orderkey = l_orderkey)",
                TPCH_CATALOG));

    final Expression.Subquery subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getSubquery();

    assertTrue(subquery.hasSetPredicate());
    final FilterRel setPredicateFilter =
        subquery
            .getSetPredicate()
            .getTuples()
            .getProject()
            .getInput()
            .getFilter(); // unique (select ... from orders where o_orderkey = l_orderkey)

    assertTrue(subquery.hasSetPredicate());
    assertSame(PredicateOp.PREDICATE_OP_UNIQUE, subquery.getSetPredicate().getPredicateOp());

    final Expression.FieldReference correlatedCol =
        setPredicateFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_orderkey

    assertEquals(0, correlatedCol.getDirectReference().getStructField().getField());
    assertEquals(1, correlatedCol.getOuterReference().getStepsOut());
  }

  @Test
  void inPredicateCorrelatedSubQuery() throws IOException, SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
    final String sql =
        "select l_orderkey from lineitem where l_partkey in (select p_partkey from part where p_partkey = l_partkey)";
    final Plan plan = toProto(s.convert(sql, TPCH_CATALOG));

    final Expression.Subquery subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getSubquery();

    assertTrue(subquery.hasInPredicate());
    final FilterRel insubqueryFilter =
        subquery
            .getInPredicate()
            .getHaystack()
            .getProject()
            .getInput()
            .getFilter(); // p_partkey = l_partkey

    final Expression.FieldReference correlatedCol =
        insubqueryFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_partkey

    assertEquals(1, correlatedCol.getDirectReference().getStructField().getField());
    assertEquals(1, correlatedCol.getOuterReference().getStepsOut());
  }

  @Test
  void notInPredicateCorrelatedSubquery() throws IOException, SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
    final String sql =
        "select l_orderkey from lineitem where l_partkey not in (select p_partkey from part where p_partkey = l_partkey)";
    final Plan plan = toProto(s.convert(sql, TPCH_CATALOG));
    final Expression.Subquery subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getScalarFunction()
            .getArguments(0)
            .getValue()
            .getSubquery();

    assertTrue(subquery.hasInPredicate());
    final FilterRel insubqueryFilter =
        subquery
            .getInPredicate()
            .getHaystack()
            .getProject()
            .getInput()
            .getFilter(); // p_partkey = l_partkey

    final Expression.FieldReference correlatedCol =
        insubqueryFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_partkey

    assertEquals(1, correlatedCol.getDirectReference().getStructField().getField());
    assertEquals(1, correlatedCol.getOuterReference().getStepsOut());
  }

  @Test
  void existsNestedCorrelatedSubquery() throws IOException, SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
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
    final Plan plan = toProto(s.convert(sql, TPCH_CATALOG));

    final Expression.Subquery outer_subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition() // exists( select ...)
            .getSubquery();

    assertTrue(outer_subquery.hasSetPredicate());
    assertSame(PredicateOp.PREDICATE_OP_EXISTS, outer_subquery.getSetPredicate().getPredicateOp());

    final FilterRel exists_filter =
        outer_subquery
            .getSetPredicate()
            .getTuples()
            .getFilter(); // l.l_partkey = p.p_partkey and unique (...)

    final Expression.Subquery inner_subquery =
        exists_filter.getCondition().getScalarFunction().getArguments(1).getValue().getSubquery();
    assertTrue(inner_subquery.hasSetPredicate());

    assertSame(PredicateOp.PREDICATE_OP_UNIQUE, inner_subquery.getSetPredicate().getPredicateOp());

    final Expression inner_subquery_condition =
        inner_subquery
            .getSetPredicate()
            .getTuples()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition();

    final Expression inner_subquery_cond1 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(0)
            .getValue(); // ps.ps_partkey = p.p_partkey
    final Expression inner_subquery_cond2 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(1)
            .getValue(); // PS.ps_suppkey = l.l_suppkey

    final Expression.FieldReference correlatedCol1 =
        inner_subquery_cond1
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // p.p_partkey
    assertEquals(0, correlatedCol1.getDirectReference().getStructField().getField());
    assertEquals(2, correlatedCol1.getOuterReference().getStepsOut());

    final Expression.FieldReference correlatedCol2 =
        inner_subquery_cond2
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l.l_suppkey
    assertEquals(2, correlatedCol2.getDirectReference().getStructField().getField());
    assertEquals(1, correlatedCol2.getOuterReference().getStepsOut());
  }

  @Test
  void nestedScalarCorrelatedSubquery() throws IOException, SqlParseException {
    final SqlToSubstrait s = new SqlToSubstrait();
    final String sql = asString("subquery/nested_scalar_subquery_in_filter.sql");
    final Plan plan = toProto(s.convert(sql, TPCH_CATALOG));
    final String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    System.out.println(planText);

    final Expression.Subquery outer_subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition() // p_size < ( select ...)
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSubquery();

    assertTrue(outer_subquery.hasScalar());

    final Expression.Subquery inner_subquery =
        outer_subquery
            .getScalar()
            .getInput()
            .getAggregate()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSubquery();

    final Expression inner_subquery_condition =
        inner_subquery.getScalar().getInput().getAggregate().getInput().getFilter().getCondition();

    final Expression inner_subquery_cond1 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(0)
            .getValue(); // ps.ps_partkey = p.p_partkey
    final Expression inner_subquery_cond2 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(1)
            .getValue(); // PS.ps_suppkey = l.l_suppkey

    final Expression.FieldReference correlatedCol1 =
        inner_subquery_cond1
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // p.p_partkey
    assertEquals(0, correlatedCol1.getDirectReference().getStructField().getField());
    assertEquals(2, correlatedCol1.getOuterReference().getStepsOut());

    final Expression.FieldReference correlatedCol2 =
        inner_subquery_cond2
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l.l_suppkey
    assertEquals(2, correlatedCol2.getDirectReference().getStructField().getField());
    assertEquals(1, correlatedCol2.getOuterReference().getStepsOut());
  }

  @Test
  void correlatedScalarSubQueryInSelect() throws Exception {
    final String sql = asString("subquery/nested_scalar_subquery_in_select.sql");
    assertSqlSubstraitRelRoundTrip(sql);
  }
}
