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
  void charLength(String column) throws Exception {
    String query = String.format("SELECT char_length(%s) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"vc32"})
  void concat(String column) throws Exception {
    String query = String.format("SELECT %s || %s FROM strings", column, column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void lower(String column) throws Exception {
    String query = String.format("SELECT lower(%s) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void upper(String column) throws Exception {
    String query = String.format("SELECT upper(%s) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void replace(String column) throws Exception {
    String query =
        String.format("SELECT replace(%s, replace_from, replace_to) FROM replace_strings", column);
    assertFullRoundTrip(query, REPLACE_CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringWith1Param(String column) throws Exception {
    String query = String.format("SELECT substring(%s, 42) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringWith2Params(String column) throws Exception {
    String query = String.format("SELECT substring(%s, 42, 5) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringFrom(String column) throws Exception {
    String query = String.format("SELECT substring(%s FROM 42) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringFromFor(String column) throws Exception {
    String query = String.format("SELECT substring(%s FROM 42 FOR 5) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trim(String column) throws Exception {
    String query = String.format("SELECT TRIM(%s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimSpecifiedCharacter(String column) throws Exception {
    String query = String.format("SELECT TRIM(' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimBoth(String column) throws Exception {
    String query = String.format("SELECT TRIM(BOTH FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimBothSpecifiedCharacter(String column) throws Exception {
    String query = String.format("SELECT TRIM(BOTH ' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimLeading(String column) throws Exception {
    String query = String.format("SELECT TRIM(LEADING FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimLeadingSpecifiedCharacter(String column) throws Exception {
    String query = String.format("SELECT TRIM(LEADING ' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimTrailing(String column) throws Exception {
    String query = String.format("SELECT TRIM(TRAILING FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void trimTrailingSpecifiedCharacter(String column) throws Exception {
    String query = String.format("SELECT TRIM(TRAILING ' ' FROM %s) FROM strings", column);
    assertSqlRoundTrip(query);
  }

  private void assertSqlRoundTrip(String sql) throws SqlParseException {
    Plan plan = assertProtoPlanRoundrip(sql, new SqlToSubstrait(), CREATES);
    assertDoesNotThrow(() -> toSql(plan), "Substrait plan to SQL");
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testStarts_With(String left, String right) throws Exception {

    String query = String.format("SELECT STARTS_WITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testStarts_WithLiteral(String left, String right) throws Exception {
    String query = String.format("SELECT STARTS_WITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testStartsWith(String left, String right) throws Exception {

    String query = String.format("SELECT STARTSWITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testStartsWithLiteral(String left, String right) throws Exception {
    String query = String.format("SELECT STARTSWITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testEnds_With(String left, String right) throws Exception {

    String query = String.format("SELECT ENDS_WITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testEnds_WithLiteral(String left, String right) throws Exception {
    String query = String.format("SELECT ENDS_WITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testEndsWith(String left, String right) throws Exception {

    String query = String.format("SELECT ENDSWITH(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testEndsWithLiteral(String left, String right) throws Exception {
    String query = String.format("SELECT ENDSWITH(%s, %s) FROM strings", left, right);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testContains(String left, String right) throws Exception {

    String query = String.format("SELECT CONTAINS_SUBSTR(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testContainsWithLiteral(String left, String right) throws Exception {

    String query = String.format("SELECT CONTAINS_SUBSTR(%s, %s) FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testPosition(String left, String right) throws Exception {

    String query = String.format("SELECT POSITION(%s IN %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testPositionWithLiteral(String left, String right) throws Exception {

    String query = String.format("SELECT POSITION(%s IN %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testStrpos(String left, String right) throws Exception {

    String query = String.format("SELECT STRPOS(%s, %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'start', vc", "vc, 'end'"},
      quoteCharacter = '`')
  void testStrposWithLiteral(String left, String right) throws Exception {

    String query = String.format("SELECT STRPOS(%s, %s) > 0 FROM strings", left, right);

    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32", "vc, i32"})
  void testLeft(String left, String right) throws Exception {

    String query = String.format("SELECT LEFT(%s, %s) FROM int_num_strings", left, right);

    assertFullRoundTrip(query, CHAR_INT_CREATES);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32", "vc, i32"})
  void testRight(String left, String right) throws Exception {

    String query = String.format("SELECT RIGHT(%s, %s) FROM int_num_strings", left, right);

    assertFullRoundTrip(query, CHAR_INT_CREATES);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32, vc32", "vc, i32, vc"})
  void testRpad(String left, String center, String right) throws Exception {

    String query =
        String.format("SELECT RPAD(%s, %s, %s) FROM int_num_strings", left, center, right);

    assertFullRoundTrip(query, CHAR_INT_CREATES);
  }

  @ParameterizedTest
  @CsvSource({"vc32, i32, vc32", "vc, i32, vc"})
  void testLpad(String left, String center, String right) throws Exception {

    String query =
        String.format("SELECT LPAD(%s, %s, %s) FROM int_num_strings", left, center, right);

    assertFullRoundTrip(query, CHAR_INT_CREATES);
  }
}
