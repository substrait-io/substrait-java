package io.substrait.isthmus;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LogarithmicFunctionTest extends PlanTestBase {

  static String CREATES =
      "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, fp32 REAL, fp64 DOUBLE)";

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void ln(String column) throws Exception {
    String query = String.format("SELECT ln(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void log10(String column) throws Exception {
    String query = String.format("SELECT log10(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i64", "fp32", "fp64"})
  void log2(String column) throws Exception {
    String query = String.format("SELECT log2(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }
}
