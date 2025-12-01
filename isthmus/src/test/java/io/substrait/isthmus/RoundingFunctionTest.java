package io.substrait.isthmus;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RoundingFunctionTest extends PlanTestBase {

  static String CREATES =
      "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, fp32 REAL, fp64 DOUBLE)";

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void ceil(String column) throws Exception {
    String query = String.format("SELECT ceil(%s) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void floor(String column) throws Exception {
    String query = String.format("SELECT floor(%s) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void round(String column) throws Exception {
    String query = String.format("SELECT round(%s, 2) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }
}
