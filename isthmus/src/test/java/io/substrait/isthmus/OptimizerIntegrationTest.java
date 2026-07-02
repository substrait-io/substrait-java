package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Project;
import io.substrait.relation.ProtoRelConverter;
import io.substrait.relation.Rel;
import io.substrait.relation.RelProtoConverter;
import io.substrait.type.Type;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.util.ImmutableBitSet;
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
    String query =
        "select e1.l_orderkey from lineitem e1 "
            + "where e1.l_quantity <> some ("
            + "  select l_quantity from lineitem e2 where e2.l_partkey = e1.l_partkey"
            + ")";

    RelRoot relRoot = SubstraitSqlToCalcite.convertQuery(query, TPCH_CATALOG);
    HepPlanner hepPlanner =
        new HepPlanner(
            new HepProgramBuilder()
                .addRuleInstance(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE)
                .build());
    hepPlanner.setRoot(relRoot.rel);
    RelNode decorrelated =
        RelDecorrelator.decorrelateQuery(
            hepPlanner.findBestExp(),
            RelFactories.LOGICAL_BUILDER.create(relRoot.rel.getCluster(), null));

    // Pin the trigger so a future Calcite bump can't silently stop exercising this path.
    assertTrue(containsLiteralAgg(decorrelated), "test setup no longer produces LITERAL_AGG");

    io.substrait.plan.Plan.Root planRoot =
        assertDoesNotThrow(
            () ->
                SubstraitRelVisitor.convert(
                    RelRoot.of(decorrelated, relRoot.kind), EXTENSION_COLLECTION));

    // The fix inserts a Project directly over the Aggregate; inspect THAT, not the outer SELECT.
    Project wrapper =
        findProjectOverAggregate(planRoot.getInput())
            .orElseThrow(() -> new AssertionError("expected a Project wrapping the Aggregate"));

    assertTrue(
        wrapper.getExpressions().stream()
            .anyMatch(
                e ->
                    e instanceof io.substrait.expression.Expression.BoolLiteral
                        && ((io.substrait.expression.Expression.BoolLiteral) e).value()),
        "LITERAL_AGG(true) should be re-inserted as a boolean true literal");

    // Passthroughs must carry the scalar field type, not the whole aggregate struct.
    assertTrue(
        wrapper.getRecordType().fields().stream().noneMatch(f -> f instanceof Type.Struct),
        "wrapper columns must be scalar; fields=" + wrapper.getRecordType().fields());

    // The wrapper subtree must survive a proto round-trip with its schema intact.
    ExtensionCollector ec = new ExtensionCollector();
    io.substrait.proto.Rel proto = new RelProtoConverter(ec).toProto(wrapper);
    Rel rt = new ProtoRelConverter(ec, extensions).from(proto);
    assertEquals(wrapper.getRecordType(), rt.getRecordType(), "wrapper schema must round-trip");
  }

  @Test
  void literalAggCombinedWithGroupingSetsIsRejected() {
    RelNode input = builder.values(new String[] {"a", "b"}, 1, 2, 3, 4).build();
    RexBuilder rexBuilder = creator.rex();
    AggregateCall literalAgg =
        AggregateCall.create(
            SqlInternalOperators.LITERAL_AGG,
            false,
            false,
            false,
            List.of(rexBuilder.makeLiteral(true)),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            "li");
    ImmutableBitSet g0 = ImmutableBitSet.of(0);
    ImmutableBitSet g1 = ImmutableBitSet.of(1);
    RelNode aggregate =
        LogicalAggregate.create(
            input, List.of(), g0.union(g1), List.of(g0, g1), List.of(literalAgg));

    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                SubstraitRelVisitor.convert(
                    RelRoot.of(aggregate, org.apache.calcite.sql.SqlKind.SELECT),
                    EXTENSION_COLLECTION));
    assertTrue(ex.getMessage().contains("GROUPING SETS"), ex.getMessage());
  }

  /**
   * Reproduces the crash from the review comment: LITERAL_AGG first in the agg-call list, followed
   * by GROUP_ID, with multiple grouping sets. The remap loop used to leave a gap and throw
   * IndexOutOfBoundsException; now it should throw the clean UnsupportedOperationException before
   * reaching the remap work.
   */
  @Test
  void literalAggBeforeGroupIdWithGroupingSetsIsRejected() {
    RelNode input = builder.values(new String[] {"a", "b"}, 1, 2, 3, 4).build();
    RexBuilder rexBuilder = creator.rex();
    AggregateCall literalAgg =
        AggregateCall.create(
            SqlInternalOperators.LITERAL_AGG,
            false,
            false,
            false,
            List.of(rexBuilder.makeLiteral(true)),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BOOLEAN),
            "li");
    AggregateCall groupIdCall =
        AggregateCall.create(
            SqlStdOperatorTable.GROUP_ID,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "gid");
    ImmutableBitSet g0 = ImmutableBitSet.of(0);
    ImmutableBitSet g1 = ImmutableBitSet.of(1);
    // LITERAL_AGG at index 0, GROUP_ID at index 1 — the ordering that triggered the crash
    RelNode aggregate =
        LogicalAggregate.create(
            input, List.of(), g0.union(g1), List.of(g0, g1), List.of(literalAgg, groupIdCall));

    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                SubstraitRelVisitor.convert(
                    RelRoot.of(aggregate, org.apache.calcite.sql.SqlKind.SELECT),
                    EXTENSION_COLLECTION));
    assertTrue(ex.getMessage().contains("GROUPING SETS"), ex.getMessage());
  }

  private static boolean containsLiteralAgg(RelNode node) {
    if (node instanceof org.apache.calcite.rel.core.Aggregate agg) {
      if (agg.getAggCallList().stream()
          .anyMatch(c -> c.getAggregation().getKind() == SqlKind.LITERAL_AGG)) {
        return true;
      }
    }
    return node.getInputs().stream().anyMatch(OptimizerIntegrationTest::containsLiteralAgg);
  }

  private static Optional<Project> findProjectOverAggregate(Rel rel) {
    if (rel instanceof Project p && p.getInput() instanceof Aggregate) {
      return Optional.of(p);
    }
    return rel.getInputs().stream()
        .map(OptimizerIntegrationTest::findProjectOverAggregate)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .findFirst();
  }
}
