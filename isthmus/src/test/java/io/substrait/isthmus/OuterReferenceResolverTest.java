package io.substrait.isthmus;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
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
}
