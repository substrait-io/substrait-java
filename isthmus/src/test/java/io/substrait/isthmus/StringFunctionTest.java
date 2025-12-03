package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.substrait.plan.Plan;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

final class StringFunctionTest extends PlanTestBase {

  static String CREATES = "CREATE TABLE strings (c16 CHAR(16), vc32 VARCHAR(32), vc VARCHAR)";
  static String REPLACE_CREATES =
      "CREATE TABLE replace_strings (c16 CHAR(16), vc32 VARCHAR(32), replace_from VARCHAR(16), replace_to VARCHAR(16))";
  static String CHAR_INT_CREATES =
      "CREATE TABLE int_num_strings (vc32 VARCHAR(32), vc VARCHAR, i32 INT)";

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void charLength(final String column) throws Exception {
    final String query = String.format("SELECT char_length(%s) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"vc32"})
  void concat(final String column) throws Exception {
    final String query = String.format("SELECT %s || %s FROM strings", column, column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void lower(final String column) throws Exception {
    final String query = String.format("SELECT lower(%s) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void upper(final String column) throws Exception {
    final String query = String.format("SELECT upper(%s) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void replace(final String column) throws Exception {
    final String query =
        String.format("SELECT replace(%s, replace_from, replace_to) FROM replace_strings", column);
    assertSqlSubstraitRelRoundTrip(query, REPLACE_CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringWith1Param(final String column) throws Exception {
    final String query = String.format("SELECT substring(%s, 42) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringWith2Params(final String column) throws Exception {
    final String query = String.format("SELECT substring(%s, 42, 5) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringFrom(final String column) throws Exception {
    final String query = String.format("SELECT substring(%s FROM 42) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringFromFor(final String column) throws Exception {
    final String query = String.format("SELECT substring(%s FROM 42 FOR 5) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trim(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(%s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimSpecifiedCharacter(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimBoth(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(BOTH FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimBothSpecifiedCharacter(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(BOTH ' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimLeading(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(LEADING FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimLeadingSpecifiedCharacter(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(LEADING ' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimTrailing(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(TRAILING FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimTrailingSpecifiedCharacter(final String column) throws Exception {
    final String query = String.format("SELECT TRIM(TRAILING ' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  private void assertSqlRoundTrip(final String sql) throws SqlParseException {
    final Plan plan = assertProtoPlanRoundrip(sql, new SqlToSubstrait(), CREATES);
    assertDoesNotThrow(() -> toSql(plan), "Substrait plan to SQL");
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testStarts_With(final String left, final String right) throws Exception {

    final String query = String.format("SELECT STARTS_WITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testStarts_WithLiteral(final String left, final String right) throws Exception {
    final String query = String.format("SELECT STARTS_WITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testStartsWith(final String left, final String right) throws Exception {

    final String query = String.format("SELECT STARTSWITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testStartsWithLiteral(final String left, final String right) throws Exception {
    final String query = String.format("SELECT STARTSWITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testEnds_With(final String left, final String right) throws Exception {

    final String query = String.format("SELECT ENDS_WITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testEnds_WithLiteral(final String left, final String right) throws Exception {
    final String query = String.format("SELECT ENDS_WITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testEndsWith(final String left, final String right) throws Exception {

    final String query = String.format("SELECT ENDSWITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testEndsWithLiteral(final String left, final String right) throws Exception {
    final String query = String.format("SELECT ENDSWITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testContains(final String left, final String right) throws Exception {

    final String query = String.format("SELECT CONTAINS_SUBSTR(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testContainsWithLiteral(final String left, final String right) throws Exception {

    final String query = String.format("SELECT CONTAINS_SUBSTR(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testPosition(final String left, final String right) throws Exception {

    final String query = String.format("SELECT POSITION(%s IN %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testPositionWithLiteral(final String left, final String right) throws Exception {

    final String query = String.format("SELECT POSITION(%s IN %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testStrpos(final String left, final String right) throws Exception {

    final String query = String.format("SELECT STRPOS(%s, %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testStrposWithLiteral(final String left, final String right) throws Exception {

    final String query = String.format("SELECT STRPOS(%s, %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32", "vc, i32"})
  void testLeft(final String left, final String right) throws Exception {

    final String query = String.format("SELECT LEFT(%s, %s) FROM int_num_strings", left, right);

    assertSqlSubstraitRelRoundTrip(query, CHAR_INT_CREATES);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32", "vc, i32"})
  void testRight(final String left, final String right) throws Exception {

    final String query = String.format("SELECT RIGHT(%s, %s) FROM int_num_strings", left, right);

    assertSqlSubstraitRelRoundTrip(query, CHAR_INT_CREATES);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32, vc32", "vc, i32, vc"})
  void testRpad(final String left, final String center, final String right) throws Exception {

    final String query =
        String.format("SELECT RPAD(%s, %s, %s) FROM int_num_strings", left, center, right);

    assertSqlSubstraitRelRoundTrip(query, CHAR_INT_CREATES);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32, vc32", "vc, i32, vc"})
  void testLpad(final String left, final String center, final String right) throws Exception {

    final String query =
        String.format("SELECT LPAD(%s, %s, %s) FROM int_num_strings", left, center, right);

    assertSqlSubstraitRelRoundTrip(query, CHAR_INT_CREATES);
  }
}
