package io.substrait.isthmus;

import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class StringFunctionTest extends PlanTestBase {

  static List<String> CREATES = List.of("CREATE TABLE strings (c16 CHAR(16), vc32 VARCHAR(32))");
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
}
