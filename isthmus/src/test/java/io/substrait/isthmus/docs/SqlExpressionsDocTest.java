package io.substrait.isthmus.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SqlExpressionToSubstrait;
import io.substrait.proto.ExtendedExpression;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/isthmus/sql-expressions.md}. Regions marked with {@code //
 * --8<-- [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet
 * includes.
 */
class SqlExpressionsDocTest extends PlanTestBase {

  @Test
  void singleExpression() throws Exception {
    // --8<-- [start:example]
    List<String> schema = List.of("CREATE TABLE lineitem (L_ORDERKEY BIGINT, L_COMMENT VARCHAR)");

    ExtendedExpression expr = new SqlExpressionToSubstrait().convert("L_ORDERKEY > 10", schema);
    // --8<-- [end:example]
    assertNotNull(expr);
  }

  @Test
  void severalExpressions() throws Exception {
    List<String> schema = List.of("CREATE TABLE lineitem (L_ORDERKEY BIGINT, L_COMMENT VARCHAR)");
    // --8<-- [start:several]
    String[] expressions = {
      "L_ORDERKEY",
      "L_ORDERKEY > 10",
      "L_ORDERKEY + 10",
      "L_ORDERKEY IN (10, 20)",
      "L_ORDERKEY IS NOT NULL"
    };

    ExtendedExpression expr = new SqlExpressionToSubstrait().convert(expressions, schema);
    // --8<-- [end:several]
    assertNotNull(expr);
  }
}
