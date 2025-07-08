package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import java.io.IOException;
import java.util.List;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;

public class OptimizerIntegrationTest extends PlanTestBase {

  private static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      SimpleExtension.loadDefaults();

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
}
