package io.substrait.isthmus;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OuterReferenceResolverTest extends PlanTestBase {
  private final RelBuilder tpcDsRelBuilder = new RelCreator(TPCDS_CATALOG).createRelBuilder();

  private static void validateOuterRef(
      final Map<RexFieldAccess, Integer> fieldAccessDepthMap,
      final String refName,
      final String colName,
      final int depth) {
    final Optional<Entry<RexFieldAccess, Integer>> entry =
        fieldAccessDepthMap.entrySet().stream()
            .filter(f -> f.getKey().getReferenceExpr().toString().equals(refName))
            .filter(f -> f.getKey().getField().getName().equals(colName))
            .filter(f -> f.getValue() == depth)
            .findFirst();
    Assertions.assertTrue(entry.isPresent());
  }

  private static Map<RexFieldAccess, Integer> buildOuterFieldRefMap(final RelNode root) {
    final OuterReferenceResolver resolver = new OuterReferenceResolver();
    return resolver.apply(root);
  }

  @Test
  void lateralJoinQuery() throws SqlParseException {
    // SQL equivalent:
    //   SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
    //   FROM store_sales CROSS JOIN LATERAL
    //     (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)
    final Holder<RexCorrelVariable> cor0 = Holder.empty();
    final RelNode calciteRel =
        tpcDsRelBuilder
            .scan("tpcds", "STORE_SALES")
            .variable(cor0::set)
            .scan("tpcds", "ITEM")
            .filter(
                tpcDsRelBuilder.equals(
                    tpcDsRelBuilder.field("I_ITEM_SK"),
                    tpcDsRelBuilder.field(cor0.get(), "SS_ITEM_SK")))
            .project(tpcDsRelBuilder.field("I_ITEM_SK"))
            .correlate(JoinRelType.INNER, cor0.get().id, tpcDsRelBuilder.field(2, 0, "SS_ITEM_SK"))
            .project(
                tpcDsRelBuilder.field("SS_SOLD_DATE_SK"),
                tpcDsRelBuilder.field("SS_ITEM_SK"),
                tpcDsRelBuilder.field("SS_CUSTOMER_SK"))
            .build();

    final Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(calciteRel);
    Assertions.assertEquals(1, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK", 1);
  }

  @Test
  void outerApplyQuery() throws SqlParseException {
    // SQL equivalent:
    //   SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
    //   FROM store_sales OUTER APPLY
    //     (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)
    final Holder<RexCorrelVariable> cor0 = Holder.empty();
    final RelNode calciteRel =
        tpcDsRelBuilder
            .scan("tpcds", "STORE_SALES")
            .variable(cor0::set)
            .scan("tpcds", "ITEM")
            .filter(
                tpcDsRelBuilder.equals(
                    tpcDsRelBuilder.field("I_ITEM_SK"),
                    tpcDsRelBuilder.field(cor0.get(), "SS_ITEM_SK")))
            .project(tpcDsRelBuilder.field("I_ITEM_SK"))
            .correlate(JoinRelType.LEFT, cor0.get().id, tpcDsRelBuilder.field(2, 0, "SS_ITEM_SK"))
            .project(
                tpcDsRelBuilder.field("SS_SOLD_DATE_SK"),
                tpcDsRelBuilder.field("SS_ITEM_SK"),
                tpcDsRelBuilder.field("SS_CUSTOMER_SK"))
            .build();

    final Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(calciteRel);
    Assertions.assertEquals(1, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK", 1);
  }

  @Test
  void nestedApplyJoinQuery() throws SqlParseException {
    // SQL equivalent:
    //   SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
    //   FROM store_sales CROSS APPLY
    //     ( SELECT i_item_sk
    //       FROM item CROSS APPLY
    //         ( SELECT p_promo_sk\
    //           FROM promotion\
    //           WHERE p_item_sk = i_item_sk AND p_item_sk = ss_item_sk )
    //       WHERE item.i_item_sk = store_sales.ss_item_sk )

    final Holder<RexCorrelVariable> cor0 = Holder.empty();
    final Holder<RexCorrelVariable> cor1 = Holder.empty();
    final RelNode calciteRel =
        tpcDsRelBuilder
            .scan("tpcds", "STORE_SALES")
            .variable(cor0::set)
            .scan("tpcds", "ITEM")
            .variable(cor1::set)
            .scan("tpcds", "PROMOTION")
            .filter(
                tpcDsRelBuilder.and(
                    tpcDsRelBuilder.equals(
                        tpcDsRelBuilder.field("P_ITEM_SK"),
                        tpcDsRelBuilder.field(cor1.get(), "I_ITEM_SK")),
                    tpcDsRelBuilder.equals(
                        tpcDsRelBuilder.field("P_ITEM_SK"),
                        tpcDsRelBuilder.field(cor0.get(), "SS_ITEM_SK"))))
            .project(tpcDsRelBuilder.field("P_PROMO_SK"))
            .correlate(JoinRelType.INNER, cor1.get().id, tpcDsRelBuilder.field(2, 0, "I_ITEM_SK"))
            .filter(
                tpcDsRelBuilder.equals(
                    tpcDsRelBuilder.field("I_ITEM_SK"),
                    tpcDsRelBuilder.field(cor0.get(), "SS_ITEM_SK")))
            .project(tpcDsRelBuilder.field("I_ITEM_SK"))
            .correlate(JoinRelType.INNER, cor0.get().id, tpcDsRelBuilder.field(2, 0, "SS_ITEM_SK"))
            .project(
                tpcDsRelBuilder.field("SS_SOLD_DATE_SK"),
                tpcDsRelBuilder.field("SS_ITEM_SK"),
                tpcDsRelBuilder.field("SS_CUSTOMER_SK"))
            .build();

    final Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(calciteRel);
    Assertions.assertEquals(3, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK", 1);
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK", 2);
    validateOuterRef(fieldAccessDepthMap, "$cor1", "I_ITEM_SK", 1);
  }

  /**
   * Regression test for the partially-decorrelated Filter case.
   *
   * <p>When a Calcite optimizer (e.g. HepPlanner) rewrites a correlated IN-subquery into a join, it
   * can produce a {@link org.apache.calcite.rel.core.Filter} whose condition contains a {@link
   * RexFieldAccess} referencing a {@link RexCorrelVariable} ({@code $cor0.field}), but whose {@link
   * RelNode#getVariablesSet()} is empty — because the enclosing {@link
   * org.apache.calcite.rel.core.Correlate} node has already been replaced by a regular join.
   *
   * <p>Before the fix, {@code OuterReferenceResolver.visit(Filter)} only iterated {@code
   * filter.getVariablesSet()}, so the {@link org.apache.calcite.rel.core.CorrelationId} was never
   * registered in {@code nestedDepth}. The subsequent {@code rexVisitor.visitFieldAccess()} call
   * found {@code !nestedDepth.containsKey(id)} and silently skipped the access, producing an empty
   * {@code fieldAccessDepthMap} instead of the expected entry.
   *
   * <p>The fix pre-scans the Filter condition for {@link RexCorrelVariable} references and
   * registers their IDs before delegating to the rex visitor.
   */
  @Test
  void filterWithCorrelVariableButEmptyVariablesSet() throws SqlParseException {
    // Build the partially-decorrelated pattern:
    //   JOIN (inner side has a Filter whose condition references $cor0 but variablesSet is empty)
    //
    // SQL equivalent of what a post-optimisation plan looks like:
    //   SELECT ss_sold_date_sk FROM store_sales
    //   JOIN item ON item.i_item_sk = store_sales.ss_item_sk   ← decorrelated join
    //   WHERE item.i_item_sk = $cor0.SS_ITEM_SK               ← Filter still has the correl ref
    //
    // We deliberately call .filter(condition) without passing the CorrelationId so that
    // getVariablesSet() returns an empty set, reproducing the post-decorrelation shape.

    final Holder<RexCorrelVariable> cor0 = Holder.empty();

    // Push STORE_SALES and capture the correlation variable for it.
    tpcDsRelBuilder.scan("tpcds", "STORE_SALES").variable(cor0::set);

    // Push ITEM on top, then build the filter condition while ITEM is the top-of-stack.
    // The condition equates item.I_ITEM_SK to the correlated $cor0.SS_ITEM_SK.
    tpcDsRelBuilder.scan("tpcds", "ITEM");
    RexNode correlCondition =
        tpcDsRelBuilder.equals(
            tpcDsRelBuilder.field("I_ITEM_SK"), tpcDsRelBuilder.field(cor0.get(), "SS_ITEM_SK"));

    final RelNode calciteRel =
        tpcDsRelBuilder
            // Intentionally NOT passing cor0.get().id to filter() → getVariablesSet() is empty.
            .filter(correlCondition)
            .join(JoinRelType.INNER, tpcDsRelBuilder.literal(true))
            .project(tpcDsRelBuilder.field("SS_SOLD_DATE_SK"))
            .build();

    // Resolver registers the dangling correlation (no longer silently dropped → no NPE).
    final Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(calciteRel);
    Assertions.assertEquals(1, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK", 0);

    // steps_out=0 is not a valid Substrait outer reference (proto requires >= 1),
    // so conversion must fail clearly rather than emit an invalid plan.
    UnsupportedOperationException ex =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(calciteRel, converterProvider));
    Assertions.assertTrue(ex.getMessage().contains("steps_out"));
  }
}
