package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.util.JsonFormat;
import io.substrait.proto.Expression;
import io.substrait.proto.FilterRel;
import io.substrait.proto.Plan;
import java.io.IOException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SubqueryPlanTest extends PlanTestBase {
  // TODO: Add a roundtrip test once the ProtoRelConverter is committed and updated to support
  // subqueries
  @Test
  public void existsCorrelatedSubquery() throws SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    Plan plan =
        s.execute(
            "select l_partkey from lineitem where exists (select o_orderdate from orders where o_orderkey = l_orderkey)",
            TPCH_CATALOG);

    Expression.Subquery subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getSubquery();

    assertTrue(subquery.hasSetPredicate());
    assertTrue(
        subquery.getSetPredicate().getPredicateOp()
            == Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_EXISTS);

    FilterRel setPredicateFilter =
        subquery
            .getSetPredicate()
            .getTuples()
            .getFilter(); // exits (select ... from orders where o_orderkey = l_orderkey)

    Expression.FieldReference correlatedCol =
        setPredicateFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_orderkey

    assertTrue(correlatedCol.getDirectReference().getStructField().getField() == 0);
    assertTrue(correlatedCol.getOuterReference().getStepsOut() == 1);
  }

  @Test
  public void uniqueCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    Plan plan =
        s.execute(
            "select l_partkey from lineitem where unique (select o_orderdate from orders where o_orderkey = l_orderkey)",
            TPCH_CATALOG);

    Expression.Subquery subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getSubquery();

    assertTrue(subquery.hasSetPredicate());
    FilterRel setPredicateFilter =
        subquery
            .getSetPredicate()
            .getTuples()
            .getProject()
            .getInput()
            .getFilter(); // unique (select ... from orders where o_orderkey = l_orderkey)

    assertTrue(subquery.hasSetPredicate());
    assertTrue(
        subquery.getSetPredicate().getPredicateOp()
            == Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_UNIQUE);

    Expression.FieldReference correlatedCol =
        setPredicateFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_orderkey

    assertTrue(correlatedCol.getDirectReference().getStructField().getField() == 0);
    assertTrue(correlatedCol.getOuterReference().getStepsOut() == 1);
  }

  @Test
  public void inPredicateCorrelatedSubQuery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String sql =
        "select l_orderkey from lineitem where l_partkey in (select p_partkey from part where p_partkey = l_partkey)";
    Plan plan = s.execute(sql, TPCH_CATALOG);

    Expression.Subquery subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition()
            .getSubquery();

    assertTrue(subquery.hasInPredicate());
    FilterRel insubqueryFilter =
        subquery
            .getInPredicate()
            .getHaystack()
            .getProject()
            .getInput()
            .getFilter(); // p_partkey = l_partkey

    Expression.FieldReference correlatedCol =
        insubqueryFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_partkey

    assertTrue(correlatedCol.getDirectReference().getStructField().getField() == 1);
    assertTrue(correlatedCol.getOuterReference().getStepsOut() == 1);
  }

  @Test
  public void notInPredicateCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String sql =
        "select l_orderkey from lineitem where l_partkey not in (select p_partkey from part where p_partkey = l_partkey)";
    Plan plan = s.execute(sql, TPCH_CATALOG);
    Expression.Subquery subquery =
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
    FilterRel insubqueryFilter =
        subquery
            .getInPredicate()
            .getHaystack()
            .getProject()
            .getInput()
            .getFilter(); // p_partkey = l_partkey

    Expression.FieldReference correlatedCol =
        insubqueryFilter
            .getCondition()
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l_partkey

    assertTrue(correlatedCol.getDirectReference().getStructField().getField() == 1);
    assertTrue(correlatedCol.getOuterReference().getStepsOut() == 1);
  }

  @Test
  public void existsNestedCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
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
    Plan plan = s.execute(sql, TPCH_CATALOG);

    Expression.Subquery outer_subquery =
        plan.getRelations(0)
            .getRoot()
            .getInput()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition() // exists( select ...)
            .getSubquery();

    assertTrue(outer_subquery.hasSetPredicate());
    assertTrue(
        outer_subquery.getSetPredicate().getPredicateOp()
            == Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_EXISTS);

    FilterRel exists_filter =
        outer_subquery
            .getSetPredicate()
            .getTuples()
            .getFilter(); // l.l_partkey = p.p_partkey and unique (...)

    Expression.Subquery inner_subquery =
        exists_filter.getCondition().getScalarFunction().getArguments(1).getValue().getSubquery();
    assertTrue(inner_subquery.hasSetPredicate());

    assertTrue(
        inner_subquery.getSetPredicate().getPredicateOp()
            == Expression.Subquery.SetPredicate.PredicateOp.PREDICATE_OP_UNIQUE);

    Expression inner_subquery_condition =
        inner_subquery
            .getSetPredicate()
            .getTuples()
            .getProject()
            .getInput()
            .getFilter()
            .getCondition();

    Expression inner_subquery_cond1 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(0)
            .getValue(); // ps.ps_partkey = p.p_partkey
    Expression inner_subquery_cond2 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(1)
            .getValue(); // PS.ps_suppkey = l.l_suppkey

    Expression.FieldReference correlatedCol1 =
        inner_subquery_cond1
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // p.p_partkey
    assertTrue(correlatedCol1.getDirectReference().getStructField().getField() == 0);
    assertTrue(correlatedCol1.getOuterReference().getStepsOut() == 2);

    Expression.FieldReference correlatedCol2 =
        inner_subquery_cond2
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l.l_suppkey
    assertTrue(correlatedCol2.getDirectReference().getStructField().getField() == 2);
    assertTrue(correlatedCol2.getOuterReference().getStepsOut() == 1);
  }

  @Test
  public void nestedScalarCorrelatedSubquery() throws IOException, SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    String sql = asString("subquery/nested_scalar_subquery_in_filter.sql");
    Plan plan = s.execute(sql, TPCH_CATALOG);
    String planText = JsonFormat.printer().includingDefaultValueFields().print(plan);

    System.out.println(planText);

    Expression.Subquery outer_subquery =
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

    Expression.Subquery inner_subquery =
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

    Expression inner_subquery_condition =
        inner_subquery.getScalar().getInput().getAggregate().getInput().getFilter().getCondition();

    Expression inner_subquery_cond1 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(0)
            .getValue(); // ps.ps_partkey = p.p_partkey
    Expression inner_subquery_cond2 =
        inner_subquery_condition
            .getScalarFunction()
            .getArguments(1)
            .getValue(); // PS.ps_suppkey = l.l_suppkey

    Expression.FieldReference correlatedCol1 =
        inner_subquery_cond1
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // p.p_partkey
    assertTrue(correlatedCol1.getDirectReference().getStructField().getField() == 0);
    assertTrue(correlatedCol1.getOuterReference().getStepsOut() == 2);

    Expression.FieldReference correlatedCol2 =
        inner_subquery_cond2
            .getScalarFunction()
            .getArguments(1)
            .getValue()
            .getSelection(); // l.l_suppkey
    assertTrue(correlatedCol2.getDirectReference().getStructField().getField() == 2);
    assertTrue(correlatedCol2.getOuterReference().getStepsOut() == 1);
  }

  @Test
  public void correlatedScalarSubQInSelect() throws IOException {
    SqlToSubstrait s = new SqlToSubstrait();
    String sql = asString("subquery/nested_scalar_subquery_in_select.sql");
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> {
          s.execute(sql, TPCH_CATALOG);
        });
  }
}
