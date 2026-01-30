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
 * Custom mapping for the Calcite {@code PARSE_TIMESTAMP} function to the Substrait {@code
 * strptime_timestamp} function.
 *
 * <p>Calcite {@code PARSE_TIMESTAMP} has <em>format</em> followed by <em>timestamp_string</em>
 * parameters, while Substrait {@code strptime_timestamp} has <em>timestamp_string</em> followed by
 * <em>format</em>. When mapping between Calcite and Substrait, the parameters need to be reversed.
 *
 * <p>{@code PARSE_TIMESTAMP(format, timestamp_string)} maps to {@code
 * strptime_timestamp(timestamp_string, format)}.
 */
public final class StrptimeTimestampFunctionMapper implements ScalarFunctionMapper {
  private static final String STRPTIME_TIMESTAMP_FUNCTION_NAME = "strptime_timestamp";
  private final List<ScalarFunctionVariant> strptimeTimestampFunctions;

  public StrptimeTimestampFunctionMapper(List<ScalarFunctionVariant> functions) {
    strptimeTimestampFunctions =
        functions.stream()
            .filter(f -> STRPTIME_TIMESTAMP_FUNCTION_NAME.equals(f.name()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (!SqlLibraryOperators.PARSE_TIMESTAMP.equals(call.op)) {
      return Optional.empty();
    }

    List<RexNode> operands = new ArrayList<>(call.getOperands());
    Collections.swap(operands, 0, 1);

    return Optional.of(
        new SubstraitFunctionMapping(
            STRPTIME_TIMESTAMP_FUNCTION_NAME, operands, strptimeTimestampFunctions));
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(ScalarFunctionInvocation expression) {
    if (!STRPTIME_TIMESTAMP_FUNCTION_NAME.equals(expression.declaration().name())) {
      return Optional.empty();
    }

    List<FunctionArg> arguments = new ArrayList<>(expression.arguments());
    Collections.swap(arguments, 0, 1);

    return Optional.of(arguments);
  }
}
