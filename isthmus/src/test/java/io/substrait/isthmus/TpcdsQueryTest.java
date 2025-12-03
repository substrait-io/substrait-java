package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.substrait.plan.Plan;
import java.io.IOException;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** TPC-DS test to convert SQL to Substrait and then convert those plans back to SQL. */
class TpcdsQueryTest extends PlanTestBase {
  private static final Set<Integer> alternateForms = Set.of(27, 36, 70, 86);

  static IntStream testCases() {
    return IntStream.rangeClosed(1, 99);
  }

  /**
   * Note that this test does not currently validate the correctness of the Substrait plan; just
   * that the SQL can be converted to Substrait and back to SQL without error.
   */
  @ParameterizedTest
  @MethodSource("testCases")
  void testQuery(final int query) throws IOException {
    final String inputSql = asString(inputSqlFile(query));
    final Plan plan = assertDoesNotThrow(() -> toSubstraitPlan(inputSql), "SQL to Substrait POJO");
    assertDoesNotThrow(() -> toSql(plan), "Substrait POJO to SQL");
    final io.substrait.proto.Plan proto =
        assertDoesNotThrow(() -> toProto(plan), "Substrait POJO to Substrait PROTO");
    assertDoesNotThrow(() -> toSql(proto), "Substrait PROTO to SQL");
  }

  private String inputSqlFile(final int query) {
    if (alternateForms.contains(query)) {
      return String.format("tpcds/queries/%02da.sql", query);
    }

    return String.format("tpcds/queries/%02d.sql", query);
  }

  private Plan toSubstraitPlan(final String sql) throws SqlParseException {
    return toSubstraitPlan(sql, TPCDS_CATALOG);
  }
}
