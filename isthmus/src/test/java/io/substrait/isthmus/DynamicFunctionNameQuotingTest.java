package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.plan.Plan;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class DynamicFunctionNameQuotingTest extends PlanTestBase {
  private SimpleExtension.ExtensionCollection declaration(String family, String name) {
    return SimpleExtension.load(
        "urn: extension:test:dynamic_names\n"
            + family
            + "_functions:\n"
            + "  - name: \""
            + name
            + "\"\n"
            + "    impls:\n"
            + "      - args:\n"
            + "          - value: i32\n"
            + "        return: i32\n");
  }

  @ParameterizedTest
  @CsvSource({
    "scalar, select",
    "scalar, has space",
    "scalar, x.y",
    "aggregate, order",
    "aggregate, has space",
    "aggregate, x.y"
  })
  void dynamicNamesParseAndBindAfterSqlExport(String family, String name) throws Exception {
    SimpleExtension.ExtensionCollection extra = declaration(family, name);
    ConverterProvider provider =
        new AutomaticDynamicFunctionMappingConverterProvider(
            ConverterProvider.builder().extensions(extra));
    PlanTestBase harness = new PlanTestBase(provider);
    String query = "SELECT \"" + name + "\"(a) FROM numbers";
    String creates = "CREATE TABLE numbers (a INTEGER)";
    Plan plan =
        new SqlToSubstrait(provider)
            .convert(
                query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(creates));
    String sql = harness.toSql(plan);
    assertTrue(sql.contains("\"" + name + "\"("), sql);
    assertNotNull(SqlParser.create(sql).parseQuery());
    Plan reimported =
        new SqlToSubstrait(provider)
            .convert(sql, SubstraitCreateStatementParser.processCreateStatementsToCatalog(creates));
    assertEquals(plan, reimported);
    harness.assertFullRoundTrip(sql, creates);
  }

  @ParameterizedTest
  @CsvSource({
    "scalar, select",
    "scalar, contains",
    "aggregate, order",
    "window, where",
    "scalar, has space",
    "aggregate, has space",
    "window, has space",
    "scalar, x.y",
    "aggregate, x.y",
    "window, x.y"
  })
  void dynamicOperatorsTreatNamesAsSingleIdentifiers(String family, String name) throws Exception {
    SqlOperator operator = SimpleExtensionToSqlOperator.from(declaration(family, name)).get(0);
    String sql =
        operator
            .createCall(SqlParserPos.ZERO, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO))
            .toSqlString(org.apache.calcite.sql.dialect.CalciteSqlDialect.DEFAULT)
            .getSql();
    assertTrue(sql.startsWith("\"" + name + "\"("), sql);
    assertNotNull(SqlParser.create("SELECT " + sql).parseQuery());
    assertEquals(name, operator.getName());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "DISTINCT "})
  void reservedAggregatePreservesItsInvocation(String quantifier) throws Exception {
    ConverterProvider provider =
        new AutomaticDynamicFunctionMappingConverterProvider(
            ConverterProvider.builder().extensions(declaration("aggregate", "order")));
    PlanTestBase harness = new PlanTestBase(provider);
    String query = "SELECT \"order\"(" + quantifier + "a) FROM numbers";
    String creates = "CREATE TABLE numbers (a INTEGER)";
    Plan plan =
        new SqlToSubstrait(provider)
            .convert(
                query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(creates));
    String sql = harness.toSql(plan);
    assertEquals(
        plan,
        new SqlToSubstrait(provider)
            .convert(
                sql, SubstraitCreateStatementParser.processCreateStatementsToCatalog(creates)));
    harness.assertFullRoundTrip(sql, creates);
  }

  @ParameterizedTest
  @CsvSource({"scalar", "aggregate", "window"})
  void ordinaryNamesKeepTheirExistingSql(String family) {
    SqlOperator operator =
        SimpleExtensionToSqlOperator.from(declaration(family, "my_function")).get(0);
    String sql =
        operator
            .createCall(SqlParserPos.ZERO, SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO))
            .toSqlString(org.apache.calcite.sql.dialect.CalciteSqlDialect.DEFAULT)
            .getSql();
    assertEquals("MY_FUNCTION(1)", sql);
  }
}
