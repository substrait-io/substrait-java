package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.google.common.collect.ImmutableSet;
import io.substrait.proto.Plan;
import java.io.IOException;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** TPC-DS test to convert SQL to Substrait and then convert those plans back to SQL. */
public class TpcdsQueryTest extends PlanTestBase {
  private static final Set<Integer> toSubstraitExclusions = ImmutableSet.of(9, 27, 36, 70, 86);
  private static final Set<Integer> fromSubstraitExclusions = ImmutableSet.of(6, 8, 67);

  static IntStream testCases() {
    return IntStream.rangeClosed(1, 99).filter(n -> !toSubstraitExclusions.contains(n));
  }

  /**
   * Note that this test does not currently validate the correctness of the Substrait plan; just
   * that the SQL can be converted to Substrait and back to SQL without error.
   */
  @ParameterizedTest
  @MethodSource("testCases")
  public void testQuery(int query) throws IOException {
    String inputSql = asString(String.format("tpcds/queries/%02d.sql", query));

    Plan plan = assertDoesNotThrow(() -> toSubstraitPlan(inputSql), "SQL to Substrait");

    if (!fromSubstraitExclusions.contains(query)) {
      assertDoesNotThrow(() -> toSql(plan), "Substrait to SQL");
    }
  }

  private Plan toSubstraitPlan(String sql) throws SqlParseException {
    return toSubstraitPlan(sql, TPCDS_CATALOG);
  }
}
