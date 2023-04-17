package io.substrait.isthmus;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class RoundingFunctionTest extends PlanTestBase {

  static List<String> CREATES =
      List.of(
          "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, fp32 FLOAT, fp64 DOUBLE)");

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
