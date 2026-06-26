package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import java.io.IOException;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.junit.jupiter.api.Test;

class OptimizerIntegrationTest extends PlanTestBase {

  private static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;

  @Test
  void conversionHandlesBuiltInSum0CallAddedByRule() throws SqlParseException, IOException {
    String query =
        "select O_CUSTKEY, count(distinct O_ORDERKEY), count(*) from orders group by O_CUSTKEY";
    // verify that the query works generally
    assertFullRoundTrip(query);

    RelRoot relRoot = SubstraitSqlToCalcite.convertQuery(query, TPCH_CATALOG);
    RelNode originalPlan = relRoot.rel;

    // Create a program to apply the AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN rule.
    // This will introduce a SqlSumEmptyIsZeroAggFunction to the plan.
    // This function does not have a mapping to Substrait.
    // SubstraitSumEmptyIsZeroAggFunction is the variant which has a mapping.
    // See io.substrait.isthmus.AggregateFunctions for details
    HepProgram program =
        new HepProgramBuilder()
            .addRuleInstance(CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN)
            .build();
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(originalPlan);
    RelNode newPlan = planner.findBestExp();

    assertDoesNotThrow(
        () ->
            // Conversion of the new plan should succeed
            SubstraitRelVisitor.convert(RelRoot.of(newPlan, relRoot.kind), EXTENSION_COLLECTION));
  }

  /**
   * Regression test for LITERAL_AGG handling in SubstraitRelVisitor.
   *
   * <p>Calcite's SubQueryRemoveRule (CALCITE-6945, landed in 1.38.0) rewrites correlated quantified
   * comparisons (e.g. {@code <> SOME}) using {@code LITERAL_AGG(true)} as a null-presence
   * indicator. SubstraitRelVisitor has no Substrait binding for {@code LITERAL_AGG}, so the
   * conversion previously crashed with "UnsupportedOperationException: Unable to find binding for
   * call LITERAL_AGG(true)".
   *
   * @see <a href="https://github.com/apache/calcite/pull/4296">CALCITE-6945 PR</a>
   */
  @Test
  void conversionHandlesLiteralAggInsertedBySubQueryRemoveRule()
      throws SqlParseException, IOException {
    // <> SOME with a correlated nullable column triggers SubQueryRemoveRule's
    // quantified-comparison path, which inserts LITERAL_AGG(true) into the aggregate.
    String query =
        "select e1.l_orderkey from lineitem e1 "
            + "where e1.l_quantity <> some ("
            + "  select l_quantity from lineitem e2 where e2.l_partkey = e1.l_partkey"
            + ")";

    RelRoot relRoot = SubstraitSqlToCalcite.convertQuery(query, TPCH_CATALOG);

    // Step 1 — SubQueryRemoveRule: rewrites RexSubQuery → LogicalCorrelate + LITERAL_AGG.
    HepProgram subQueryProgram =
        new HepProgramBuilder().addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE).build();
    HepPlanner hepPlanner = new HepPlanner(subQueryProgram);
    hepPlanner.setRoot(relRoot.rel);
    RelNode afterSubQueryRemove = hepPlanner.findBestExp();

    // Step 2 — RelDecorrelator: rewrites LogicalCorrelate → LEFT JOIN; LITERAL_AGG survives in
    // the aggregate as a synthetic null-presence indicator column.
    RelNode decorrelated =
        RelDecorrelator.decorrelateQuery(
            afterSubQueryRemove,
            RelFactories.LOGICAL_BUILDER.create(relRoot.rel.getCluster(), null));

    // Conversion must succeed and produce the correct output schema.
    // The query selects a single column (l_orderkey), so the plan root must expose 1 field.
    // The LITERAL_AGG wrapper emits a Project on top of the aggregate; verify that structure.
    io.substrait.plan.Plan.Root planRoot =
        assertDoesNotThrow(
            () ->
                SubstraitRelVisitor.convert(
                    RelRoot.of(decorrelated, relRoot.kind), EXTENSION_COLLECTION));
    Rel result = planRoot.getInput();

    // The outermost Rel visible to the caller is a Project that re-inserts the LITERAL_AGG
    // literal and passes real measures through — it must expose exactly 1 output field
    // (l_orderkey) matching the SELECT list.
    assertInstanceOf(Project.class, result, "expected LITERAL_AGG wrapper Project at plan root");
    assertEquals(
        1,
        result.getRecordType().fields().size(),
        "output schema should have exactly 1 field (l_orderkey)");
  }
}
