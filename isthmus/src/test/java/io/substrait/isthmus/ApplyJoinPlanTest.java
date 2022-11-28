package io.substrait.isthmus;

import java.util.Map;
import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ApplyJoinPlanTest {

  private static RelRoot getCalcitePlan(SqlToSubstrait s, TpcdsSchema schema, String sql)
      throws SqlParseException {
    var pair = s.registerSchema("tpcds", schema);
    var converter = s.createSqlToRelConverter(pair.left, pair.right);
    SqlParser parser = SqlParser.create(sql, s.parserConfig);
    var root = s.getBestExpRelRoot(converter, parser.parseQuery());
    return root;
  }

  private static void validateOuterRef(
      Map<RexFieldAccess, Integer> fieldAccessDepthMap, String refName, String colName) {
    var rexField =
        fieldAccessDepthMap.keySet().stream()
            .filter(f -> f.getReferenceExpr().toString().equals(refName))
            .findFirst();
    Assertions.assertTrue(rexField.isPresent());
    Assertions.assertEquals(colName, rexField.get().getField().getKey());
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
    TpcdsSchema schema = new TpcdsSchema(1.0);
    String sql;
    sql =
        """
            SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
            FROM store_sales CROSS JOIN LATERAL
              (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)""";

    /* the calcite plan for the above query is:
      LogicalProject(SS_SOLD_DATE_SK=[$0], SS_ITEM_SK=[$2], SS_CUSTOMER_SK=[$3])
       LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{2}])
         LogicalTableScan(table=[[tpcds, STORE_SALES]])
         LogicalProject(I_ITEM_SK=[$0])
           LogicalFilter(condition=[=($0, $cor0.SS_ITEM_SK)])
             LogicalTableScan(table=[[tpcds, ITEM]])
    */

    // validate outer reference map
    RelRoot root = getCalcitePlan(new SqlToSubstrait(), schema, sql);
    Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(root);
    Assertions.assertEquals(1, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK");

    // TODO validate end to end conversion
    var sE2E = new SqlToSubstrait();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> sE2E.execute(sql, "tpcds", schema),
        "Lateral join is not supported");
  }

  @Test
  public void outerApplyQuery() throws SqlParseException {
    TpcdsSchema schema = new TpcdsSchema(1.0);
    String sql;
    sql =
        """
            SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
            FROM store_sales OUTER APPLY
              (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)""";

    FeatureBoard featureBoard =
        ImmutableFeatureBoard.builder()
            .sqlConformanceMode(SqlConformanceEnum.SQL_SERVER_2008)
            .build();
    SqlToSubstrait s = new SqlToSubstrait(featureBoard);
    RelRoot root = getCalcitePlan(s, schema, sql);

    Map<RexFieldAccess, Integer> fieldAccessDepthMap = buildOuterFieldRefMap(root);
    Assertions.assertEquals(1, fieldAccessDepthMap.size());
    validateOuterRef(fieldAccessDepthMap, "$cor0", "SS_ITEM_SK");

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> s.execute(sql, "tpcds", schema),
        "APPLY is not supported");
  }

  @Test
  public void crossApplyQuery() throws SqlParseException {
    TpcdsSchema schema = new TpcdsSchema(1.0);
    String sql;
    sql =
        """
            SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
            FROM store_sales CROSS APPLY
              (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)""";

    FeatureBoard featureBoard =
        ImmutableFeatureBoard.builder()
            .sqlConformanceMode(SqlConformanceEnum.SQL_SERVER_2008)
            .build();
    SqlToSubstrait s = new SqlToSubstrait(featureBoard);

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> s.execute(sql, "tpcds", schema),
        "APPLY is not supported");
  }
}
