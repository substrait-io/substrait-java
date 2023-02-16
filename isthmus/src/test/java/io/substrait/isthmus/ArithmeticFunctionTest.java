package io.substrait.isthmus;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ArithmeticFunctionTest extends PlanTestBase {

  static List<String> CREATES =
      List.of(
          "CREATE TABLE INTS (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT)",
          "CREATE TABLE FLOATS (fp32 FLOAT, fp64 DOUBLE)");

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void integerArithmetic(String c) throws Exception {
    String query =
        String.format(
            "SELECT %s + %s, %s - %s, %s * %s, %s / %s FROM ints", c, c, c, c, c, c, c, c);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void floatingPointArithmetic(String c) throws Exception {
    String query =
        String.format(
            "SELECT %s + %s, %s - %s, %s * %s, %s / %s FROM floats", c, c, c, c, c, c, c, c);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void negation() throws Exception {
    assertSqlSubstraitRelRoundTrip("SELECT -i8, -i16, -i32, -i64 FROM ints", CREATES);
    assertSqlSubstraitRelRoundTrip("SELECT -fp32, -fp64 FROM floats", CREATES);
  }
}
