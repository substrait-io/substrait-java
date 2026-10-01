package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.isthmus.sql.SubstraitSqlToCalcite;
import io.substrait.plan.Plan;
import io.substrait.relation.Project;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.TimestampString;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class DatetimeBindingRegressionTest extends PlanTestBase {

  private static final String CREATES =
      "CREATE TABLE events (ts9 TIMESTAMP(9), ts3 TIMESTAMP(3), i INTEGER)";

  @ParameterizedTest
  @ValueSource(strings = {"+", "-"})
  void widenedIntervalLiteralRoundTrips(String operator) throws Exception {
    assertFullRoundTrip("SELECT ts9 " + operator + " INTERVAL '5' DAY FROM events", CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.123", "-0.123"})
  void widenedIntervalLiteralPreservesSubseconds(String value) throws Exception {
    String query = "SELECT ts9 + INTERVAL '" + value + "' SECOND FROM events";
    Plan plan =
        new SqlToSubstrait()
            .convert(
                query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES));
    Project project = assertInstanceOf(Project.class, plan.getRoots().get(0).getInput());
    Expression.ScalarFunctionInvocation call =
        assertInstanceOf(
            Expression.ScalarFunctionInvocation.class, project.getExpressions().get(0));
    assertEquals(
        ExpressionCreator.intervalDay(
            false, 0, 0, value.startsWith("-") ? -123_000_000 : 123_000_000, 9),
        call.arguments().get(1));
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @CsvSource({
    "TIMESTAMP, 2024-01-01 00:00:00.123, 1704067200123000000",
    "TIMESTAMP, 1969-12-31 23:59:59.123, -877000000",
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE, 2024-01-01 00:00:00.123, 1704067200123000000",
    "TIMESTAMP_WITH_LOCAL_TIME_ZONE, 1969-12-31 23:59:59.123, -877000000"
  })
  void widenedTimestampLiteralPreservesValue(
      SqlTypeName typeName, String timestamp, long expected) {
    RexBuilder rex = new RexBuilder(converterProvider.getTypeFactory());
    RexNode literal =
        rex.makeLiteral(
            new TimestampString(timestamp),
            converterProvider.getTypeFactory().createSqlType(typeName, 3),
            true);
    Expression.Cast result =
        assertInstanceOf(Expression.Cast.class, timestampPlusInterval(literal));
    Expression.ScalarFunctionInvocation call =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, result.input());
    assertEquals(
        typeName == SqlTypeName.TIMESTAMP
            ? ExpressionCreator.precisionTimestamp(false, expected, 9)
            : ExpressionCreator.precisionTimestampTZ(false, expected, 9),
        call.arguments().get(0));
  }

  @ParameterizedTest
  @EnumSource(
      value = SqlTypeName.class,
      names = {"TIMESTAMP", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"})
  void overflowingTimestampLiteralFailsOnConversion(SqlTypeName typeName) {
    RexBuilder rex = new RexBuilder(converterProvider.getTypeFactory());
    RexNode literal =
        rex.makeLiteral(
            new TimestampString("9999-12-31 00:00:00"),
            converterProvider.getTypeFactory().createSqlType(typeName, 3),
            true);
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> timestampPlusInterval(literal));
    assertTrue(error.getMessage().contains("precision 9"), error.getMessage());
    assertTrue(error.getMessage().contains("64-bit"), error.getMessage());
  }

  @ParameterizedTest
  @EnumSource(
      value = SqlTypeName.class,
      names = {"TIMESTAMP", "TIMESTAMP_WITH_LOCAL_TIME_ZONE"})
  void widenedTimestampNullRemainsNull(SqlTypeName typeName) {
    RexBuilder rex = new RexBuilder(converterProvider.getTypeFactory());
    RexNode literal =
        rex.makeNullLiteral(converterProvider.getTypeFactory().createSqlType(typeName, 3));
    Expression.Cast result =
        assertInstanceOf(Expression.Cast.class, timestampPlusInterval(literal));
    Expression.ScalarFunctionInvocation call =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, result.input());
    assertEquals(
        ExpressionCreator.typedNull(
            typeName == SqlTypeName.TIMESTAMP
                ? N.precisionTimestamp(9)
                : N.precisionTimestampTZ(9)),
        call.arguments().get(0));
  }

  private Expression timestampPlusInterval(RexNode timestamp) {
    RexBuilder rex = new RexBuilder(converterProvider.getTypeFactory());
    RexNode interval =
        rex.makeInputRef(
            converterProvider
                .getTypeFactory()
                .createSqlIntervalType(
                    new SqlIntervalQualifier(
                        TimeUnit.DAY, 10, TimeUnit.SECOND, 9, SqlParserPos.ZERO)),
            0);
    return rex.makeCall(timestamp.getType(), SqlStdOperatorTable.PLUS, List.of(timestamp, interval))
        .accept(new RexExpressionConverter(converterProvider.getScalarFunctionConverter()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"+", "-"})
  void widenedIntervalExpressionProducesParseableSql(String operator) throws Exception {
    String query = "SELECT ts9 " + operator + " i * INTERVAL '1' DAY FROM events";
    Plan plan =
        new SqlToSubstrait()
            .convert(
                query, SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES));
    String sql = toSql(plan);
    assertTrue(sql.contains("AS INTERVAL DAY"), sql);
    SubstraitSqlToCalcite.convertQuery(
        sql,
        SubstraitCreateStatementParser.processCreateStatementsToCatalog(CREATES),
        converterProvider);
    assertFullRoundTrip(query, CREATES);
  }

  @ParameterizedTest
  @ValueSource(strings = {"TIMESTAMP(3)", "TIMESTAMP(3) WITH LOCAL TIME ZONE"})
  void timestampPrecisionAboveConfiguredLimitFailsOnConversion(String inputType) throws Exception {
    ConverterProvider provider =
        ConverterProvider.builder()
            .typeFactory(new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT))
            .build();
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new SqlToSubstrait(provider)
                    .convert(
                        "SELECT ts3 - INTERVAL '5' DAY FROM events",
                        SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                            "CREATE TABLE events (ts3 " + inputType + ")")));
    assertTrue(error.getMessage().contains("precision 6"), error.getMessage());
    assertTrue(
        error.getMessage().contains("max precision in Calcite type system is set to 3"),
        error.getMessage());
  }

  @ParameterizedTest
  @CsvSource({
    "local_timestamp, TIMESTAMP(6) WITH LOCAL TIME ZONE, pts",
    "assume_timezone, TIMESTAMP(6), ptstz"
  })
  void dynamicTimezoneFunctionUsesDeclaredType(String function, String inputType, String result)
      throws Exception {
    ConverterProvider provider =
        new AutomaticDynamicFunctionMappingConverterProvider(ConverterProvider.builder());
    Plan plan =
        new SqlToSubstrait(provider)
            .convert(
                "SELECT " + function + "(ts, CAST('Asia/Tokyo' AS VARCHAR)) FROM events",
                SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                    "CREATE TABLE events (ts " + inputType + ")"));
    Project project = assertInstanceOf(Project.class, plan.getRoots().get(0).getInput());
    Expression.ScalarFunctionInvocation call =
        assertInstanceOf(
            Expression.ScalarFunctionInvocation.class, project.getExpressions().get(0));
    assertEquals(
        result.equals("pts") ? N.precisionTimestamp(6) : N.precisionTimestampTZ(6),
        call.outputType());
    new PlanTestBase(provider) {}.assertFullRoundTrip(project);
  }
}
