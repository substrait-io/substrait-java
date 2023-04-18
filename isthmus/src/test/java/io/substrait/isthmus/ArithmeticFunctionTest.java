package io.substrait.isthmus;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ArithmeticFunctionTest extends PlanTestBase {

  static List<String> CREATES =
      List.of(
          "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, fp32 FLOAT, fp64 DOUBLE)");

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void arithmetic(String c) throws Exception {
    String query =
        String.format(
            "SELECT %s + %s, %s - %s, %s * %s, %s / %s FROM numbers", c, c, c, c, c, c, c, c);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void abs(String column) throws Exception {
    String query = String.format("SELECT abs(%s) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void exponential(String column) throws Exception {
    String query = String.format("SELECT exp(%s) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void mod(String column) throws Exception {
    String query = String.format("SELECT mod(%s, %s) FROM numbers", column, column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void negation(String column) throws Exception {
    String query = String.format("SELECT -%s FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i64", "fp32", "fp64"})
  void power(String column) throws Exception {
    String query = String.format("SELECT power(%s, %s) FROM numbers", column, column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"sin", "cos", "tan", "asin", "acos", "atan"})
  void trigonometric(String fname) throws Exception {
    String query = String.format("SELECT %s(fp32), %s(fp64) FROM numbers", fname, fname);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void atan2(String column) throws Exception {
    String query = String.format("SELECT atan2(%s, %s) FROM numbers", column, column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void sign(String column) throws Exception {
    String query = String.format("SELECT sign(%s) FROM numbers", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }
}
