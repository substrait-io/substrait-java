package io.substrait.isthmus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class ComparisonFunctionsTest extends PlanTestBase {
  static String CREATES =
      "CREATE TABLE numbers (int_a INT, int_b INT, double_a DOUBLE, double_b DOUBLE)";

  @Test
  void is_true() throws Exception {
    String query = "SELECT ((int_a > int_b) IS TRUE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void is_false() throws Exception {
    String query = "SELECT ((int_a > int_b) IS FALSE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void is_not_true() throws Exception {
    String query = "SELECT ((int_a > int_b) IS NOT TRUE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void is_not_false() throws Exception {
    String query = "SELECT ((int_a > int_b) IS NOT FALSE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({"int_a, int_b", "int_b, int_a", "double_a, double_b", "double_b, double_a"})
  void is_distinct_from(String left, String right) throws Exception {
    String query = String.format("SELECT (%s IS DISTINCT FROM %s) FROM numbers", left, right);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"int_a", "int_b", "double_a", "double_b"})
  void is_distinct_from_null_vs_col(String column) throws Exception {
    String query = String.format("SELECT (NULL IS DISTINCT FROM %s) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }
}
