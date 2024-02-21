package io.substrait.isthmus;

import static io.substrait.isthmus.SqlConverterBase.EXTENSION_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

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

  @Test
  void conversionHandlesBuiltInSum0CallAddedByRule() throws SqlParseException, IOException {
    var query =
        "select O_CUSTKEY, count(distinct O_ORDERKEY), count(*) from orders group by O_CUSTKEY";
    // verify that the query works generally
    assertFullRoundTrip(query);

    SqlToSubstrait sqlConverter = new SqlToSubstrait();
    List<RelRoot> relRoots = sqlConverter.sqlToRelNode(query, tpchSchemaCreateStatements());
    assertEquals(1, relRoots.size());
    RelRoot planRoot = relRoots.get(0);
    RelNode originalPlan = planRoot.rel;

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
    var newPlan = planner.findBestExp();

    assertDoesNotThrow(
        () ->
            // Conversion of the new plan should succeed
            SubstraitRelVisitor.convert(RelRoot.of(newPlan, planRoot.kind), EXTENSION_COLLECTION));
  }
}
