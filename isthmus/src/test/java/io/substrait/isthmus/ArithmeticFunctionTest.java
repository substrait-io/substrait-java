package io.substrait.isthmus;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ArithmeticFunctionTest extends PlanTestBase {

  static List<String> CREATES =
      List.of(
          "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, fp32 REAL, fp64 DOUBLE)");

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void arithmetic(String c) throws Exception {
    String query =
        String.format(
            "SELECT %s + %s, %s - %s, %s * %s, %s / %s FROM numbers", c, c, c, c, c, c, c, c);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void abs(String column) throws Exception {
    String query = String.format("SELECT abs(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void exponential(String column) throws Exception {
    String query = String.format("SELECT exp(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void mod(String column) throws Exception {
    String query = String.format("SELECT mod(%s, %s) FROM numbers", column, column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void negation(String column) throws Exception {
    String query = String.format("SELECT -%s FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i64", "fp32", "fp64"})
  void power(String column) throws Exception {
    String query = String.format("SELECT power(%s, %s) FROM numbers", column, column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"sin", "cos", "tan", "asin", "acos", "atan"})
  void trigonometric(String fname) throws Exception {
    String query = String.format("SELECT %s(fp32), %s(fp64) FROM numbers", fname, fname);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void atan2(String column) throws Exception {
    String query = String.format("SELECT atan2(%s, %s) FROM numbers", column, column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void sign(String column) throws Exception {
    String query = String.format("SELECT sign(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void max(String column) throws Exception {
    String query = String.format("SELECT max(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void min(String column) throws Exception {
    String query = String.format("SELECT min(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void avg(String column) throws Exception {
    String query = String.format("SELECT avg(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void sum(String column) throws Exception {
    String query = String.format("SELECT sum(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void sum0(String column) throws Exception {
    String query = String.format("SELECT sum0(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }
}
