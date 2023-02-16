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
  void abs() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "SELECT abs(i8), abs(i16), abs(i32), abs(i64) FROM ints", CREATES);
    assertSqlSubstraitRelRoundTrip("SELECT abs(fp32), abs(fp64) FROM floats", CREATES);
  }

  @Test
  void exponential() throws Exception {
    assertSqlSubstraitRelRoundTrip("SELECT exp(fp32), exp(fp64) FROM floats", CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void mod(String c) throws Exception {
    String query = String.format("SELECT mod(%s, %s) FROM ints", c, c);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void negation() throws Exception {
    assertSqlSubstraitRelRoundTrip("SELECT -i8, -i16, -i32, -i64 FROM ints", CREATES);
    assertSqlSubstraitRelRoundTrip("SELECT -fp32, -fp64 FROM floats", CREATES);
  }

  @Test
  void power() throws Exception {
    assertSqlSubstraitRelRoundTrip("SELECT  power(i64, i64) FROM ints", CREATES);
    assertSqlSubstraitRelRoundTrip(
        "SELECT power(fp32, fp32), power(fp64, fp64) FROM floats", CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"sin", "cos", "tan", "asin", "acos", "atan"})
  void trigonometric(String fname) throws Exception {
    String query = String.format("SELECT %s(fp32), %s(fp64) FROM floats", fname, fname);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @Test
  void sign() throws Exception {
    assertSqlSubstraitRelRoundTrip(
        "SELECT sign(i8), sign(i16), sign(i32), sign(i64) FROM ints", CREATES);
    assertSqlSubstraitRelRoundTrip("SELECT sign(fp32), sign(fp64) FROM floats", CREATES);
  }
}
