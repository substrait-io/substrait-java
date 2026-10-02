package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.sql.SubstraitCreateStatementParser;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.type.TypeCreator;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class DateIntervalArithmeticTest extends PlanTestBase {

  @ParameterizedTest
  @CsvSource({
    "+, 36, HOUR, 1",
    "+, -36, HOUR, -1",
    "-, 36, HOUR, 1",
    "-, -36, HOUR, -1",
    "+, 23, HOUR, 0",
    "+, -23, HOUR, 0",
    "-, 23, HOUR, 0",
    "-, -23, HOUR, 0",
    "+, 2161, MINUTE, 1",
    "-, -2161, MINUTE, -1",
    "+, 86400.001, SECOND, 1",
    "-, -86400.001, SECOND, -1",
    "+, 1 12:30:45.123, DAY TO SECOND, 1",
    "-, -1 12:30:45.123, DAY TO SECOND, -1"
  })
  void subDayLiteralTruncatesTowardsZero(
      String operator, String value, String qualifier, int expectedDays) throws Exception {
    String query =
        "SELECT d " + operator + " INTERVAL '" + value + "' " + qualifier + " FROM events";
    Project project =
        assertInstanceOf(
            Project.class,
            new SqlToSubstrait()
                .convert(
                    query,
                    SubstraitCreateStatementParser.processCreateStatementsToCatalog(
                        "CREATE TABLE events (d DATE)"))
                .getRoots()
                .get(0)
                .getInput());
    Expression.Cast cast = assertInstanceOf(Expression.Cast.class, project.getExpressions().get(0));
    assertEquals(N.DATE, cast.getType());
    Expression.ScalarFunctionInvocation arithmetic =
        assertInstanceOf(Expression.ScalarFunctionInvocation.class, cast.input());
    assertEquals(
        (operator.equals("+") ? "add" : "subtract") + ":date_iday", arithmetic.declaration().key());
    assertEquals(N.precisionTimestamp(6), arithmetic.outputType());
    assertEquals(
        ExpressionCreator.intervalDay(false, expectedDays, 0, 0, 6), arithmetic.arguments().get(1));
    assertFullRoundTrip(project);
  }

  @ParameterizedTest
  @EnumSource(
      value = TimeUnitRange.class,
      names = {
        "DAY",
        "DAY_TO_HOUR",
        "DAY_TO_MINUTE",
        "DAY_TO_SECOND",
        "HOUR",
        "HOUR_TO_MINUTE",
        "HOUR_TO_SECOND",
        "MINUTE",
        "MINUTE_TO_SECOND",
        "SECOND"
      })
  void dateResultAcceptsOnlyDayQualifiedIntervalColumns(TimeUnitRange qualifier) {
    RexBuilder rex = new RexBuilder(typeFactory);
    RelDataType intervalType =
        typeFactory.createSqlIntervalType(
            new SqlIntervalQualifier(qualifier.startUnit, qualifier.endUnit, SqlParserPos.ZERO));
    RexExpressionConverter converter =
        new RexExpressionConverter(
            new ScalarFunctionConverter(extensions.scalarFunctions(), typeFactory));
    for (SqlOperator operator :
        List.of(SqlStdOperatorTable.DATETIME_PLUS, SqlStdOperatorTable.MINUS_DATE)) {
      RexNode call =
          rex.makeCall(
              typeFactory.createSqlType(SqlTypeName.DATE),
              operator,
              List.of(
                  rex.makeInputRef(typeFactory.createSqlType(SqlTypeName.DATE), 0),
                  rex.makeInputRef(intervalType, 1)));
      if (qualifier == TimeUnitRange.DAY) {
        Expression.Cast cast = assertInstanceOf(Expression.Cast.class, call.accept(converter));
        assertEquals(R.DATE, cast.getType());
        Expression.ScalarFunctionInvocation arithmetic =
            assertInstanceOf(Expression.ScalarFunctionInvocation.class, cast.input());
        assertEquals(R.precisionTimestamp(6), arithmetic.outputType());
        assertInstanceOf(FieldReference.class, arithmetic.arguments().get(1));
      } else {
        UnsupportedOperationException error =
            assertThrows(UnsupportedOperationException.class, () -> call.accept(converter));
        assertEquals(
            "DATE arithmetic with a non-literal sub-day interval is not supported",
            error.getMessage());
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"add,false", "add,true", "subtract,false", "subtract,true"})
  void timestampResultWithIntervalColumnRoundTrips(String function, boolean nullable) {
    TypeCreator types = TypeCreator.of(nullable);
    NamedScan table =
        sb.namedScan(
            List.of("events"), List.of("d", "duration"), List.of(types.DATE, types.intervalDay(6)));
    Project project =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        function + ":date_iday",
                        types.precisionTimestamp(6),
                        sb.fieldReference(input, 0),
                        sb.fieldReference(input, 1))),
            sb.remap(2),
            table);
    assertFullRoundTrip(project);
  }

  @ParameterizedTest
  @ValueSource(strings = {"add", "subtract"})
  void timestampResultKeepsSubDayLiteral(String function) {
    NamedScan table = sb.namedScan(List.of("events"), List.of("d"), List.of(R.DATE));
    Project project =
        sb.project(
            input ->
                List.of(
                    sb.scalarFn(
                        DefaultExtensionCatalog.FUNCTIONS_DATETIME,
                        function + ":date_iday",
                        R.precisionTimestamp(6),
                        sb.fieldReference(input, 0),
                        ExpressionCreator.intervalDay(false, -1, -43200, 0, 6))),
            sb.remap(1),
            table);
    assertFullRoundTrip(project);
  }
}
