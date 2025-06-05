package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.substrait.plan.Plan;
import java.util.List;
import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public final class StringFunctionTest extends PlanTestBase {

  static List<String> CREATES =
      List.of("CREATE TABLE strings (c16 CHAR(16), vc32 VARCHAR(32), vc VARCHAR)");
  static List<String> REPLACE_CREATES =
      List.of(
          "CREATE TABLE replace_strings (c16 CHAR(16), vc32 VARCHAR(32), replace_from VARCHAR(16), replace_to VARCHAR(16))");

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void charLength(String column) throws Exception {
    String query = String.format("SELECT char_length(%s) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"vc32"})
  void concat(String column) throws Exception {
    String query = String.format("SELECT %s || %s FROM strings", column, column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void lower(String column) throws Exception {
    String query = String.format("SELECT lower(%s) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void upper(String column) throws Exception {
    String query = String.format("SELECT upper(%s) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void replace(String column) throws Exception {
    String query =
        String.format("SELECT replace(%s, replace_from, replace_to) FROM replace_strings", column);
    assertSqlSubstraitRelRoundTrip(query, REPLACE_CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringWith1Param(String column) throws Exception {
    String query = String.format("SELECT substring(%s, 42) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringWith2Params(String column) throws Exception {
    String query = String.format("SELECT substring(%s, 42, 5) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringFrom(String column) throws Exception {
    String query = String.format("SELECT substring(%s FROM 42) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"c16", "vc32"})
  void substringFromFor(String column) throws Exception {
    String query = String.format("SELECT substring(%s FROM 42 FOR 5) FROM strings", column);
    assertSqlSubstraitRelRoundTrip(query, CREATES);
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
}
