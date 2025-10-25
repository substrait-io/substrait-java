package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Custom function mapper to represent power(x, 0.5) as sqrt(x) to ensure the float formats are
 * supported as defined in substrait/extensions/functions_arithmetic
 */
final class SqrtFunctionMapper implements ScalarFunctionMapper {
  private static final String sqrtFunctionName = "sqrt";
  private final List<ScalarFunctionVariant> sqrtFunctions;

  public SqrtFunctionMapper(List<ScalarFunctionVariant> functions) {
    this.sqrtFunctions =
        functions.stream()
            .filter(f -> sqrtFunctionName.equalsIgnoreCase(f.name()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (sqrtFunctions.isEmpty()) {
      return Optional.empty();
    }

    if (isPowerOfHalf(call)) {
      List<RexNode> operands = call.getOperands().subList(0, 1);
      return Optional.of(new SubstraitFunctionMapping(sqrtFunctionName, operands, sqrtFunctions));
    }

    return Optional.empty();
  }

  private static boolean isPowerOfHalf(final RexCall call) {
    if (!SqlStdOperatorTable.POWER.equals(call.getOperator()) || call.getOperands().size() != 2) {
      return false;
    }

    RexNode exponent = call.getOperands().get(1);
    while (exponent.getKind() == SqlKind.CAST) {
      exponent = ((RexCall) exponent).getOperands().get(0);
    }

    if (!(exponent instanceof RexLiteral)) {
      return false;
    }
    RexLiteral literal = (RexLiteral) exponent;

    switch (literal.getType().getSqlTypeName()) {
      case DOUBLE:
      case FLOAT:
      case REAL:
        {
          final Double digit = literal.getValueAs(Double.class);
          return digit != null && Math.abs(digit - 0.5d) < 1e-15;
        }

      case DECIMAL:
        {
          final BigDecimal bigdec = literal.getValueAs(BigDecimal.class);
          return bigdec != null && BigDecimal.valueOf(5, 1).compareTo(bigdec) == 0;
        }

      default:
        return false;
    }
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      final Expression.ScalarFunctionInvocation expression) {
    return Optional.empty();
  }
}
