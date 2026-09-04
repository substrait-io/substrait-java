package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.Expression;
import io.substrait.expression.Expression.FixedCharLiteral;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import io.substrait.relation.Project;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.api.Test;
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
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void initcap(String column) throws Exception {
    String query = String.format("SELECT initcap(%s) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32", "vc"})
  void reverse(String column) throws Exception {
    String query = String.format("SELECT reverse(%s) FROM strings", column);
    assertFullRoundTrip(query, CREATES);
  }

  /**
   * Isthmus binds its own {@code REVERSE} rather than Calcite's, whose {@code
   * ARG0_NULLABLE_VARYING} return widens a {@code CHAR} to {@code VARCHAR}. A round trip cannot see
   * that: both directions carry the recorded {@code output_type} verbatim, so a call declaring
   * {@code varchar<16>} for a {@code fixedchar<16>} operand round-trips green.
   */
  @Test
  void reverseKeepsTheDeclaredReturnOfAFixedCharOperand() throws Exception {
    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES);

    Plan plan = new SqlToSubstrait().convert("SELECT reverse(c16) FROM strings", catalog);

    Project project = (Project) plan.getRoots().get(0).getInput();
    Expression.ScalarFunctionInvocation reverse =
        (Expression.ScalarFunctionInvocation) project.getExpressions().get(0);
    assertEquals("reverse:fchar", reverse.declaration().key());
    assertEquals(TypeCreator.NULLABLE.fixedChar(16), reverse.outputType());
  }

  /**
   * Calcite names the Spark library's operator {@code REVERSE} as well, so with both libraries
   * enabled a lookup by that name returns two candidates and keeps neither. Isthmus' own operator
   * is consulted first, which is what makes these reachable.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT c16 FROM strings ORDER BY reverse(c16)",
        "SELECT reverse(c16) FROM strings ORDER BY 1",
        "SELECT reverse(c16) AS r FROM strings ORDER BY r"
      })
  void reverseIsReachableInAnOrderBy(String query) throws Exception {
    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES);

    assertDoesNotThrow(() -> new SqlToSubstrait().convert(query, catalog));
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
  void testPosition(String substring, String input) throws Exception {
    String query = String.format("SELECT POSITION(%s IN %s) FROM strings", substring, input);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"'substring', vc", "vc, 'string'"},
      quoteCharacter = '`')
  void testPositionWithLiteral(String substring, String input) throws Exception {
    String query = String.format("SELECT POSITION(%s IN %s) FROM strings", substring, input);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource({"c16, c16", "c16, vc32", "c16, vc", "vc32, vc32", "vc32, vc", "vc, vc"})
  void testStrpos(String input, String substring) throws Exception {
    String query = String.format("SELECT STRPOS(%s, %s) FROM strings", input, substring);
    assertSqlRoundTrip(query);
  }

  @ParameterizedTest
  @CsvSource(
      value = {"vc, 'substring'", "'string', vc"},
      quoteCharacter = '`')
  void testStrposWithLiteral(String input, String substring) throws Exception {
    String query = String.format("SELECT STRPOS(%s, %s) FROM strings", input, substring);
    assertSqlRoundTrip(query);
  }

  @Test
  // Calcite POSITION(substring in input) maps to Substrait strpos(input, substring).
  // Calcite represents STRPOS as POSITION so this test covers both functions.
  void testPositionParameterOrdering() throws Exception {
    String input = "input";
    String substring = "substring";
    String sql = String.format("SELECT POSITION('%s' in '%s') FROM strings", substring, input);
    CalciteCatalogReader catalog =
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES);

    Plan plan = new SqlToSubstrait().convert(sql, catalog);

    List<String> expected = List.of(input, substring);

    Plan.Root root = plan.getRoots().stream().findFirst().orElseThrow();
    Project project = (Project) root.getInput();
    Expression.ScalarFunctionInvocation strpos =
        (Expression.ScalarFunctionInvocation) project.getExpressions().get(0);
    List<String> actual =
        strpos.arguments().stream()
            .map(arg -> (FixedCharLiteral) arg)
            .map(FixedCharLiteral::value)
            .toList();

    assertEquals(expected, actual);
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
