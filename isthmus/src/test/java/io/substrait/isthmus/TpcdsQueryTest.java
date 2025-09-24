package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.plan.Plan;
import java.io.IOException;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** TPC-DS test to convert SQL to Substrait and then convert those plans back to SQL. */
public class TpcdsQueryTest extends PlanTestBase {
  private static final Set<Integer> alternateForms = Set.of(27, 36, 70, 86);
  private static final Set<Integer> toSubstraitExclusions = Set.of(9);
  private static final Set<Integer> fromSubstraitPojoExclusions = Set.of(1, 30, 81);
  private static final Set<Integer> fromSubstraitProtoExclusions = Set.of(1, 30, 81);

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
    String inputSql = asString(inputSqlFile(query));

    Plan plan = assertDoesNotThrow(() -> toSubstraitPlan(inputSql), "SQL to Substrait POJO");

    if (!fromSubstraitPojoExclusions.contains(query)) {
      assertDoesNotThrow(() -> toSql(plan), "Substrait POJO to SQL");
    } else {
      assertThrows(Throwable.class, () -> toSql(plan), "Substrait POJO to SQL");
    }

    io.substrait.proto.Plan proto =
        assertDoesNotThrow(() -> toProto(plan), "Substrait POJO to Substrait PROTO");

    if (!fromSubstraitProtoExclusions.contains(query)) {
      assertDoesNotThrow(() -> toSql(proto), "Substrait PROTO to SQL");
    } else {
      assertThrows(Throwable.class, () -> toSql(proto), "Substrait PROTO to SQL");
    }
  }

  private String inputSqlFile(int query) {
    if (alternateForms.contains(query)) {
      return String.format("tpcds/queries/%02da.sql", query);
    }

    return String.format("tpcds/queries/%02d.sql", query);
  }

  private Plan toSubstraitPlan(String sql) throws SqlParseException {
    return toSubstraitPlan(sql, TPCDS_CATALOG);
  }
}
