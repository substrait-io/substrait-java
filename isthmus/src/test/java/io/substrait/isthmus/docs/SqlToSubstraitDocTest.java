package io.substrait.isthmus.docs;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.substrait.isthmus.PlanTestBase;
import io.substrait.isthmus.SqlToSubstrait;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import org.apache.calcite.prepare.Prepare;
import org.junit.jupiter.api.Test;

/**
 * Backs the code samples in {@code docs/isthmus/sql-to-substrait.md}. Regions marked with {@code //
 * --8<-- [start:name]} / {@code [end:name]} are pulled into the docs via {@code --8<--} snippet
 * includes.
 */
class SqlToSubstraitDocTest extends PlanTestBase {

  @Test
  void workedExample() throws Exception {
    // --8<-- [start:worked-example]
    String[] createStatements = {
      "CREATE TABLE users (id BIGINT, name VARCHAR, signup_date DATE)",
      "CREATE TABLE orders (order_id BIGINT, user_id BIGINT, total DECIMAL(10, 2))"
    };

    // 1. Parse the schema into a catalog.
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(createStatements);

    // 2. Convert a query into a Substrait Plan POJO.
    Plan plan =
        new SqlToSubstrait()
            .convert(
                "SELECT u.name, o.total "
                    + "FROM users u JOIN orders o ON u.id = o.user_id "
                    + "WHERE o.total > 100.00",
                catalog);

    // 3. Serialize the POJO Plan to the protobuf wire format.
    io.substrait.proto.Plan proto = new PlanProtoConverter().toProto(plan);
    // --8<-- [end:worked-example]
    assertNotNull(proto);
  }

  @Test
  void multipleStatements() throws Exception {
    Prepare.CatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
            "CREATE TABLE orders (order_id BIGINT, user_id BIGINT, total DECIMAL(10, 2))");
    // --8<-- [start:multiple-statements]
    Plan plan =
        new SqlToSubstrait()
            .convert(
                "SELECT order_id FROM orders; " + "SELECT user_id FROM orders WHERE total > 20;",
                catalog);

    // plan.getRoots() has two entries, one per SELECT.
    // --8<-- [end:multiple-statements]
    assertNotNull(plan);
  }
}
