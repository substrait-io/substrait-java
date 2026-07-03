package io.substrait.isthmus;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.SubstraitRelNodeConverter.Context;
import io.substrait.isthmus.expression.CurrentTimezoneFunction;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.RexExpressionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.type.TypeCreator;
import java.util.Collections;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.Test;

/**
 * Bi-directional conversion tests for the three Substrait execution context variables ({@code
 * current_timestamp}, {@code current_date}, {@code current_timezone}) ⇄ Calcite.
 */
class ExecutionContextVariableConversionTest extends CalciteObjs {

  protected static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION =
      DefaultExtensionCatalog.DEFAULT_COLLECTION;

  private final ScalarFunctionConverter scalarFunctionConverter =
      new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), type);

  private final ExpressionRexConverter expressionRexConverter =
      new ExpressionRexConverter(type, scalarFunctionConverter, null, TypeConverter.DEFAULT);

  private final RexExpressionConverter rexExpressionConverter = new RexExpressionConverter();

  @Test
  void currentTimestamp() {
    // Substrait current_timestamp is a precision_timestamp_tz -> Calcite
    // TIMESTAMP_WITH_LOCAL_TIME_ZONE, carrying the fractional-second precision.
    RelDataType returnType =
        TypeConverter.DEFAULT.toCalcite(type, TypeCreator.REQUIRED.precisionTimestampTZ(6));
    bitest(
        ExpressionCreator.currentTimestamp(6),
        rex.makeCall(returnType, SqlStdOperatorTable.CURRENT_TIMESTAMP, Collections.emptyList()));
  }

  @Test
  void currentDate() {
    RelDataType returnType = TypeConverter.DEFAULT.toCalcite(type, TypeCreator.REQUIRED.DATE);
    bitest(
        ExpressionCreator.currentDate(),
        rex.makeCall(returnType, SqlStdOperatorTable.CURRENT_DATE, Collections.emptyList()));
  }

  @Test
  void currentTimezone() {
    // Calcite has no built-in session-timezone operator; the Substrait-specific
    // CurrentTimezoneFunction is used instead.
    RelDataType returnType = TypeConverter.DEFAULT.toCalcite(type, TypeCreator.REQUIRED.STRING);
    bitest(
        ExpressionCreator.currentTimezone(),
        rex.makeCall(returnType, CurrentTimezoneFunction.INSTANCE, Collections.emptyList()));
  }

  // bi-directional test: 1) rex -> substrait, 2) substrait -> rex2, compare against expectations
  void bitest(Expression expression, RexNode rexNode) {
    assertEquals(expression, rexNode.accept(rexExpressionConverter));
    RexNode convertedRex = expression.accept(expressionRexConverter, Context.newContext());
    assertEquals(rexNode, convertedRex);
  }
}
