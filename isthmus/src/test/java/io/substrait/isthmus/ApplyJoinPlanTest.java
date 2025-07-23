package io.substrait.isthmus;

import java.util.Map;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ApplyJoinPlanTest extends PlanTestBase {

  private static RelRoot getCalcitePlan(String sql) throws SqlParseException {
    SqlToSubstrait s = new SqlToSubstrait();
    return s.sqlToRelNode(sql, TPCDS_CATALOG).get(0);
  }

  private static void validateOuterRef(
      Map<RexFieldAccess, Integer> fieldAccessDepthMap, String refName, String colName, int depth) {
    var entry =
        fieldAccessDepthMap.entrySet().stream()
            .filter(f -> f.getKey().getReferenceExpr().toString().equals(refName))
            .filter(f -> f.getKey().getField().getName().equals(colName))
            .filter(f -> f.getValue() == depth)
            .findFirst();
    Assertions.assertTrue(entry.isPresent());
  }

  private static Map<RexFieldAccess, Integer> buildOuterFieldRefMap(RelRoot root) {
    final OuterReferenceResolver resolver = new OuterReferenceResolver();
    var fieldAccessDepthMap = resolver.getFieldAccessDepthMap();
    Assertions.assertEquals(0, fieldAccessDepthMap.size());
    resolver.apply(root.rel);
    return fieldAccessDepthMap;
  }

  @Test
  public void lateralJoinQuery() throws SqlParseException {
    String sql;
    sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales CROSS JOIN LATERAL\n"
            + "  (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)";

    /* the calcite plan for the above query is:
      LogicalProject(SS_SOLD_DATE_SK=[$0], SS_ITEM_SK=[$2], SS_CUSTOMER_SK=[$3])
       LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{2}])
         LogicalTableScan(table=[[tpcds, STORE_SALES]])
         LogicalProject(I_ITEM_SK=[$0])
           LogicalFilter(condition=[=($0, $cor0.SS_ITEM_SK)])
             LogicalTableScan(table=[[tpcds, ITEM]])
    */

    // validate outer reference map
    RelRoot root = getCalcitePlan(sql);
    Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(root);
    Assertions.assertEquals(1, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK", 1);

    // TODO validate end to end conversion
    var sE2E = new SqlToSubstrait();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> sE2E.execute(sql, TPCDS_CATALOG),
        "Lateral join is not supported");
  }

  @Test
  public void outerApplyQuery() throws SqlParseException {
    String sql;
    sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales OUTER APPLY\n"
            + "  (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)";

    RelRoot root = getCalcitePlan(sql);

    Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(root);
    Assertions.assertEquals(1, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK", 1);

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new SqlToSubstrait().execute(sql, TPCDS_CATALOG),
        "APPLY is not supported");
  }

  @Test
  public void nestedApplyJoinQuery() throws SqlParseException {
    String sql;
    sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales CROSS APPLY\n"
            + "  ( SELECT i_item_sk\n"
            + "    FROM item CROSS APPLY\n"
            + "      ( SELECT p_promo_sk\n"
            + "        FROM promotion\n"
            + "        WHERE p_item_sk = i_item_sk AND p_item_sk = ss_item_sk )\n"
            + "    WHERE item.i_item_sk = store_sales.ss_item_sk )";

    /* the calcite plan for the above query is:
    LogicalProject(SS_SOLD_DATE_SK=[$0], SS_ITEM_SK=[$2], SS_CUSTOMER_SK=[$3])
      LogicalCorrelate(correlation=[$cor2], joinType=[inner], requiredColumns=[{2}])
        LogicalTableScan(table=[[tpcds, STORE_SALES]])
        LogicalProject(I_ITEM_SK=[$0])
          LogicalFilter(condition=[=($0, $cor2.SS_ITEM_SK)])
            LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{0}])
              LogicalTableScan(table=[[tpcds, ITEM]])
              LogicalProject(P_PROMO_SK=[$0])
                LogicalFilter(condition=[AND(=($4, $cor0.I_ITEM_SK), =($4, $cor2.SS_ITEM_SK))])
                  LogicalTableScan(table=[[tpcds, PROMOTION]])
     */
    FeatureBoard featureBoard = ImmutableFeatureBoard.builder().build();
    SqlToSubstrait s = new SqlToSubstrait(featureBoard);
    RelRoot root = getCalcitePlan(sql);

    Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(root);
    Assertions.assertEquals(3, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor2", "SS_ITEM_SK", 1);
    validateOuterRef(fieldAccessDepthMap, "$cor2", "SS_ITEM_SK", 2);
    validateOuterRef(fieldAccessDepthMap, "$cor0", "I_ITEM_SK", 1);

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new SqlToSubstrait().execute(sql, TPCDS_CATALOG),
        "APPLY is not supported");
  }

  @Test
  public void crossApplyQuery() throws SqlParseException {
    String sql;
    sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales CROSS APPLY\n"
            + "  (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)";

    FeatureBoard featureBoard = ImmutableFeatureBoard.builder().build();
    SqlToSubstrait s = new SqlToSubstrait(featureBoard);

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new SqlToSubstrait().execute(sql, TPCDS_CATALOG),
        "APPLY is not supported");
  }
}
