package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.substrait.proto.Plan;
import java.io.IOException;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** TPC-H test to convert SQL to Substrait and then convert those plans back to SQL. */
public class TpchQueryTest extends PlanTestBase {
  private static final Set<Integer> fromSubstraitExclusions = Set.of(17);

  static IntStream testCases() {
    return IntStream.rangeClosed(1, 22);
  }

  /**
   * Note that this test does not currently validate the correctness of the Substrait plan; just
   * that the SQL can be converted to Substrait and back to SQL without error.
   */
  @ParameterizedTest
  @MethodSource("testCases")
  public void testQuery(int query) throws IOException {
    String inputSql = asString(String.format("tpch/queries/%02d.sql", query));

    Plan plan = assertDoesNotThrow(() -> toSubstraitPlan(inputSql), "SQL to Substrait");

    if (!fromSubstraitExclusions.contains(query)) {
      assertDoesNotThrow(() -> toSql(plan), "Substrait to SQL");
    }
  }

  private Plan toSubstraitPlan(String sql) throws SqlParseException {
    return toSubstraitPlan(sql, TPCH_CATALOG);
  }
}
