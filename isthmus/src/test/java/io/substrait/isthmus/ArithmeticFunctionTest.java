package io.substrait.isthmus;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ArithmeticFunctionTest extends PlanTestBase {

  static String CREATES =
      "CREATE TABLE numbers (i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, fp32 REAL, fp64 DOUBLE)";

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void arithmetic(final String c) throws Exception {
    final String query =
        String.format(
            "SELECT %s + %s, %s - %s, %s * %s, %s / %s FROM numbers", c, c, c, c, c, c, c, c);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void abs(final String column) throws Exception {
    final String query = String.format("SELECT abs(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void exponential(final String column) throws Exception {
    final String query = String.format("SELECT exp(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void mod(final String column) throws Exception {
    final String query = String.format("SELECT mod(%s, %s) FROM numbers", column, column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void negation(final String column) throws Exception {
    final String query = String.format("SELECT -%s FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i64", "fp32", "fp64"})
  void power(final String column) throws Exception {
    final String query = String.format("SELECT power(%s, %s) FROM numbers", column, column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"sin", "cos", "tan", "asin", "acos", "atan"})
  void trigonometric(final String fname) throws Exception {
    final String query = String.format("SELECT %s(fp32), %s(fp64) FROM numbers", fname, fname);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void atan2(final String column) throws Exception {
    final String query = String.format("SELECT atan2(%s, %s) FROM numbers", column, column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void sign(final String column) throws Exception {
    final String query = String.format("SELECT sign(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void max(final String column) throws Exception {
    final String query = String.format("SELECT max(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void min(final String column) throws Exception {
    final String query = String.format("SELECT min(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void avg(final String column) throws Exception {
    final String query = String.format("SELECT avg(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void sum(final String column) throws Exception {
    final String query = String.format("SELECT sum(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64", "fp32", "fp64"})
  void sum0(final String column) throws Exception {
    final String query = String.format("SELECT sum0(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i64", "fp32", "fp64"})
  void sqrt(final String column) throws Exception {
    final String query = String.format("SELECT sqrt(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void sinh(final String column) throws Exception {
    final String query = String.format("SELECT SINH(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void tanh(final String column) throws Exception {
    final String query = String.format("SELECT TANH(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void cosh(final String column) throws Exception {
    final String query = String.format("SELECT COSH(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void asinh(final String column) throws Exception {
    final String query = String.format("SELECT ASINH(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void atanh(final String column) throws Exception {
    final String query = String.format("SELECT ATANH(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void acosh(final String column) throws Exception {
    final String query = String.format("SELECT ACOSH(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void bitwise_not_scalar(final String column) throws Exception {
    final String query = String.format("SELECT BITNOT(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({"i8, 8", "i16, 160", "i32, 32000", "i64, CAST(6000000004 AS BIGINT)"})
  void bitwise_and_scalar(final String column, final String mask) throws Exception {
    final String query = String.format("SELECT BITAND(%s, %s) FROM numbers", column, mask);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({"i8, 8", "i16, 160", "i32, 32000", "i64, CAST(6000000004 AS BIGINT)"})
  void bitwise_xor_scalar(final String column, final String mask) throws Exception {
    final String query = String.format("SELECT BITXOR(%s, %s) FROM numbers", column, mask);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({"i8, 8", "i16, 160", "i32, 32000", "i64, CAST(6000000004 AS BIGINT)"})
  void bitwise_or_scalar(final String column, final String mask) throws Exception {
    final String query = String.format("SELECT BITOR(%s, %s) FROM numbers", column, mask);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void radians(final String column) throws Exception {
    final String query = String.format("SELECT RADIANS(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fp32", "fp64"})
  void degrees(final String column) throws Exception {
    final String query = String.format("SELECT DEGREES(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i32", "i64"})
  void factorial(final String column) throws Exception {
    final String query = String.format("SELECT FACTORIAL(%s) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void bit_left_shift(final String column) throws Exception {
    final String query = String.format("SELECT %s << 1 FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"i8", "i16", "i32", "i64"})
  void leftshift(final String column) throws Exception {
    final String query = String.format("SELECT LEFTSHIFT(%s, 1) FROM numbers", column);
    assertFullRoundTrip(query, CREATES);
  }
}
