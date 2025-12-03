package io.substrait.isthmus;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ComparisonFunctionsTest extends PlanTestBase {
  static String CREATES =
      "CREATE TABLE numbers (int_a INT, int_b INT, int_c INT, double_a DOUBLE, double_b DOUBLE, double_c DOUBLE)";

  @Test
  void is_true() throws Exception {
    final String query = "SELECT ((int_a > int_b) IS TRUE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void is_false() throws Exception {
    final String query = "SELECT ((int_a > int_b) IS FALSE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void is_not_true() throws Exception {
    final String query = "SELECT ((int_a > int_b) IS NOT TRUE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void is_not_false() throws Exception {
    final String query = "SELECT ((int_a > int_b) IS NOT FALSE) FROM numbers";
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({"int_a, int_b", "int_b, int_a", "double_a, double_b", "double_b, double_a"})
  void is_distinct_from(final String left, final String right) throws Exception {
    final String query = String.format("SELECT (%s IS DISTINCT FROM %s) FROM numbers", left, right);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"int_a", "int_b", "double_a", "double_b"})
  void is_distinct_from_null_vs_col(final String column) throws Exception {
    final String query = String.format("SELECT (NULL IS DISTINCT FROM %s) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({
    "int_a, int_b, int_c",
    "double_a, double_b, double_c",
    "int_a, int_b",
    "int_a, int_b, double_a",
    "CAST(NULL AS INT), int_a, int_b"
  })
  void least(final String args) throws Exception {
    final String join_args = String.join(", ", args);
    final String query = String.format("SELECT LEAST(%s) FROM numbers", join_args);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({
    "int_a, int_b, int_c",
    "double_a, double_b, double_c",
    "int_a, int_b",
    "int_a, int_b, double_a",
    "CAST(NULL AS INT), int_a, int_b"
  })
  void greatest(final String args) throws Exception {
    final String join_args = String.join(", ", args);
    final String query = String.format("SELECT LEAST(%s) FROM numbers", join_args);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }
}
