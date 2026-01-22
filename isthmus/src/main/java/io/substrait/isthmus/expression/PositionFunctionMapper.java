package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Custom mapping for the Calcite {@code POSITION} function to the Substrait {@code strpos}
 * function. Calcite also represents the SQL {@code STRPOS} function as {@code POSITION}.
 *
 * <p>Calcite {@code POSITION} has <em>substring</em> followed by <em>input</em> parameters, while
 * Substrait {@code strpos} has <em>input</em> followed by <em>substring</em>. When mapping between
 * Calcite and Substrait, the parameters need to be reversed
 *
 * <p>{@code POSITION(substring IN input)} maps to {@code strpos(input, substring)}.
 */
final class PositionFunctionMapper implements ScalarFunctionMapper {
  private static final String strposFunctionName = "strpos";
  private final List<ScalarFunctionVariant> strposFunctions;

  public PositionFunctionMapper(List<ScalarFunctionVariant> functions) {
    strposFunctions =
        functions.stream()
            .filter(f -> strposFunctionName.equals(f.name()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(final RexCall call) {
    if (!SqlStdOperatorTable.POSITION.equals(call.op)) {
      return Optional.empty();
    }

    List<RexNode> operands = new ArrayList<>(call.getOperands());
    Collections.swap(operands, 0, 1);

    return Optional.of(new SubstraitFunctionMapping(strposFunctionName, operands, strposFunctions));
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      final Expression.ScalarFunctionInvocation expression) {
    if (!strposFunctionName.equals(expression.declaration().name())) {
      return Optional.empty();
    }

    List<FunctionArg> args = new ArrayList<>(expression.arguments());
    Collections.swap(args, 0, 1);
    return Optional.of(args);
  }
}
