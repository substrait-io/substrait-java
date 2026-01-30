package io.substrait.isthmus.expression;

import io.substrait.expression.Expression.ScalarFunctionInvocation;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlLibraryOperators;

/**
 * Custom mapping for the Calcite {@code PARSE_TIME} function to the Substrait {@code strptime_time}
 * function.
 *
 * <p>Calcite {@code PARSE_TIME} has <em>format</em> followed by <em>time_string</em> parameters,
 * while Substrait {@code strptime_time} has <em>time_string</em> followed by <em>format</em>. When
 * mapping between Calcite and Substrait, the parameters need to be reversed.
 *
 * <p>{@code PARSE_TIME(format, time_string)} maps to {@code strptime_time(time_string, format)}.
 */
public final class StrptimeTimeFunctionMapper implements ScalarFunctionMapper {
  private static final String STRPTIME_TIME_FUNCTION_NAME = "strptime_time";
  private final List<ScalarFunctionVariant> strptimeTimeFunctions;

  public StrptimeTimeFunctionMapper(List<ScalarFunctionVariant> functions) {
    strptimeTimeFunctions =
        functions.stream()
            .filter(f -> STRPTIME_TIME_FUNCTION_NAME.equals(f.name()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (!SqlLibraryOperators.PARSE_TIME.equals(call.op)) {
      return Optional.empty();
    }

    List<RexNode> operands = new ArrayList<>(call.getOperands());
    Collections.swap(operands, 0, 1);

    return Optional.of(
        new SubstraitFunctionMapping(STRPTIME_TIME_FUNCTION_NAME, operands, strptimeTimeFunctions));
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(ScalarFunctionInvocation expression) {
    if (!STRPTIME_TIME_FUNCTION_NAME.equals(expression.declaration().name())) {
      return Optional.empty();
    }

    List<FunctionArg> arguments = new ArrayList<>(expression.arguments());
    Collections.swap(arguments, 0, 1);

    return Optional.of(arguments);
  }
}
