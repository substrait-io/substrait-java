package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Maps Calcite datetime subtraction calls to Substrait's {@code subtract} function. */
final class DatetimeSubtractionFunctionMapper implements ScalarFunctionMapper {
  private static final String SUBTRACT_FUNCTION_NAME = "subtract";
  private final List<ScalarFunctionVariant> subtractFunctions;

  DatetimeSubtractionFunctionMapper(List<ScalarFunctionVariant> functions) {
    this.subtractFunctions =
        functions.stream()
            .filter(
                function ->
                    SUBTRACT_FUNCTION_NAME.equals(function.name())
                        && DefaultExtensionCatalog.FUNCTIONS_DATETIME.equals(function.urn()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (subtractFunctions.isEmpty() || !SqlStdOperatorTable.MINUS_DATE.equals(call.getOperator())) {
      return Optional.empty();
    }
    return Optional.of(
        new SubstraitFunctionMapping(
            SUBTRACT_FUNCTION_NAME, call.getOperands(), subtractFunctions));
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      Expression.ScalarFunctionInvocation expression) {
    return Optional.empty();
  }
}
