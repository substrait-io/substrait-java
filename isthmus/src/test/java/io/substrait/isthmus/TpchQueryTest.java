package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.substrait.plan.Plan;
import java.io.IOException;
import java.util.stream.IntStream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** TPC-H test to convert SQL to Substrait and then convert those plans back to SQL. */
class TpchQueryTest extends PlanTestBase {
  static IntStream testCases() {
    return IntStream.rangeClosed(1, 22);
  }

  /**
   * Note that this test does not currently validate the correctness of the Substrait plan; just
   * that the SQL can be converted to Substrait and back to SQL without error.
   */
  @ParameterizedTest
  @MethodSource("testCases")
  void testQuery(int query) throws IOException {
    String inputSql = asString(String.format("tpch/queries/%02d.sql", query));

    Plan plan = assertDoesNotThrow(() -> toSubstraitPlan(inputSql), "SQL to Substrait POJO");

    assertDoesNotThrow(() -> toSql(plan), "Substrait POJO to SQL");

    io.substrait.proto.Plan proto =
        assertDoesNotThrow(() -> toProto(plan), "Substrait POJO to Substrait PROTO");

    assertDoesNotThrow(() -> toSql(proto), "Substrait PROTO to SQL");
  }

  private Plan toSubstraitPlan(String sql) throws SqlParseException {
    return toSubstraitPlan(sql, TPCH_CATALOG);
  }
}
