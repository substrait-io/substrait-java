package io.substrait.isthmus;

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlToSubstraitJoinConversionTest extends PlanTestBase {
  @Test
  void lateralJoinQuery() throws SqlParseException {
    final String sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales CROSS JOIN LATERAL\n"
            + "  (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)";

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new SqlToSubstrait().convert(sql, TPCDS_CATALOG),
        "Lateral join is not supported");
  }

  @Test
  void outerApplyQuery() throws SqlParseException {
    final String sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales OUTER APPLY\n"
            + "  (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)";

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new SqlToSubstrait().convert(sql, TPCDS_CATALOG),
        "APPLY is not supported");
  }

  @Test
  void nestedApplyJoinQuery() throws SqlParseException {
    final String sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales CROSS APPLY\n"
            + "  ( SELECT i_item_sk\n"
            + "    FROM item CROSS APPLY\n"
            + "      ( SELECT p_promo_sk\n"
            + "        FROM promotion\n"
            + "        WHERE p_item_sk = i_item_sk AND p_item_sk = ss_item_sk )\n"
            + "    WHERE item.i_item_sk = store_sales.ss_item_sk )";

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new SqlToSubstrait().convert(sql, TPCDS_CATALOG),
        "APPLY is not supported");
  }

  @Test
  void crossApplyQuery() throws SqlParseException {
    final String sql =
        "SELECT ss_sold_date_sk, ss_item_sk, ss_customer_sk\n"
            + "FROM store_sales CROSS APPLY\n"
            + "  (select i_item_sk from item where item.i_item_sk = store_sales.ss_item_sk)";

    // TODO validate end to end conversion
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> new SqlToSubstrait().convert(sql, TPCDS_CATALOG),
        "APPLY is not supported");
  }
}
