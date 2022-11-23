package io.substrait.isthmus;

import org.apache.calcite.adapter.tpcds.TpcdsSchema;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ApplyJoinPlanTest {

  @Test
  public void lateralJoinQuery() {
    TpcdsSchema schema = new TpcdsSchema(1.0);
    String sql;
    sql =
        """
        SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk
        FROM store_sales CROSS JOIN LATERAL
          (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)""";

    SqlToSubstrait s = new SqlToSubstrait();
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> s.execute(sql, "tpcds", schema),
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
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> s.execute(sql, "tpcds", schema),
        "APPLY is not supported");
  }
}
