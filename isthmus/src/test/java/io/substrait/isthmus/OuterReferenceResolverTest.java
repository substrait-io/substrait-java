package io.substrait.isthmus;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OuterReferenceResolverTest extends PlanTestBase {
  private final RelBuilder tpcDsRelBuilder = new RelCreator(TPCDS_CATALOG).createRelBuilder();

  private static OuterReferenceResolver resolve(final RelNode root) {
    final OuterReferenceResolver resolver = new OuterReferenceResolver();
    resolver.resolve(root);
    return resolver;
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

    final OuterReferenceResolver resolver = resolve(calciteRel);
    // The correlation binds to the Correlate's left input, which is stamped with the anchor emitted
    // by references to $cor0.
    Assertions.assertNotNull(resolver.anchorForCorrelationId(cor0.get().id));
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

    final OuterReferenceResolver resolver = resolve(calciteRel);
    Assertions.assertNotNull(resolver.anchorForCorrelationId(cor0.get().id));
  }

  @Test
  void nestedApplyJoinQuery() throws SqlParseException {
    // SQL equivalent:
    //   SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
    //   FROM store_sales CROSS APPLY
    //     ( SELECT i_item_sk
    //       FROM item CROSS APPLY
    //         ( SELECT p_promo_sk
    //           FROM promotion
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

    final OuterReferenceResolver resolver = resolve(calciteRel);
    final Integer anchor0 = resolver.anchorForCorrelationId(cor0.get().id);
    final Integer anchor1 = resolver.anchorForCorrelationId(cor1.get().id);
    // Each correlation binds to a distinct relation, so they receive distinct anchors.
    Assertions.assertNotNull(anchor0);
    Assertions.assertNotNull(anchor1);
    Assertions.assertNotEquals(anchor0, anchor1);
  }

  /**
   * Regression test for the partially-decorrelated Filter case.
   *
   * <p>When a Calcite optimizer (e.g. HepPlanner) rewrites a correlated IN-subquery into a join, it
   * can produce a {@link org.apache.calcite.rel.core.Filter} whose condition contains a {@link
   * org.apache.calcite.rex.RexFieldAccess} referencing a {@link RexCorrelVariable} ({@code
   * $cor0.field}), but whose {@link RelNode#getVariablesSet()} is empty — because the enclosing
   * {@link org.apache.calcite.rel.core.Correlate} node has already been replaced by a regular join.
   *
   * <p>Such a correlation is never declared, so it has no binding relation. The resolver leaves it
   * unbound and conversion fails with a clear error rather than emitting an invalid or misdirected
   * outer reference.
   */
  @Test
  void filterWithCorrelVariableButEmptyVariablesSet() throws SqlParseException {
    final Holder<RexCorrelVariable> cor0 = Holder.empty();

    // Push STORE_SALES and capture the correlation variable for it.
    tpcDsRelBuilder.scan("tpcds", "STORE_SALES").variable(cor0::set);

    // Push ITEM on top, then build the filter condition while ITEM is the top-of-stack.
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

    // The dangling correlation is never declared, so it is not bound to any relation.
    final OuterReferenceResolver resolver = resolve(calciteRel);
    Assertions.assertNull(resolver.anchorForCorrelationId(cor0.get().id));

    // Conversion must fail clearly rather than emit a misdirected outer reference.
    UnsupportedOperationException ex =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () -> SubstraitRelVisitor.convert(calciteRel, converterProvider));
    Assertions.assertTrue(ex.getMessage().contains("no binding relation"));
  }
}
