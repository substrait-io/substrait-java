package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.math.BigDecimal;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Custom function mapper for sqrt to not revert to power(x, 0.5) instead to use sqrt(x) to ensure
 * the float formats are supported as defined in substrait/extensions/functions_arithmetic
 */
final class SqrtFunctionMapper implements ScalarFunctionMapper {

  private final List<ScalarFunctionVariant> sqrtFunction;

  public SqrtFunctionMapper(List<ScalarFunctionVariant> functions) {

    this.sqrtFunction = findFunction("sqrt", functions);
  }

  private List<ScalarFunctionVariant> findFunction(
      String name, Collection<ScalarFunctionVariant> functions) {

    return functions.stream()
        .filter(f -> name.equalsIgnoreCase(f.name()))
        .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {

    try {
      if (sqrtFunction.isEmpty()) return Optional.empty();

      final SqlOperator op = call.getOperator();
      if (SqlStdOperatorTable.SQRT.equals(op)) {
        return Optional.of(new SubstraitFunctionMapping("sqrt", call.getOperands(), sqrtFunction));
      }

      if (SqlStdOperatorTable.POWER.equals(op) && call.getOperands().size() == 2) {
        final RexNode base = call.getOperands().get(0);
        final RexNode exp = call.getOperands().get(1);
        if (includesPower(exp)) {
          return Optional.of(new SubstraitFunctionMapping("sqrt", List.of(base), sqrtFunction));
        }
      }

      return Optional.empty();
    } catch (Exception ex) {
      return Optional.empty();
    }
  }

  private static boolean includesPower(RexNode node) {

    while (node.getKind() == SqlKind.CAST) {
      node = ((RexCall) node).getOperands().get(0);
    }

    if (!(node instanceof RexLiteral)) {
      return false;
    }
    RexLiteral lit = (RexLiteral) node;

    final SqlTypeName type = lit.getType().getSqlTypeName();
    switch (type) {
      case DOUBLE:
      case FLOAT:
      case REAL:
        {
          final Double digit = lit.getValueAs(Double.class);
          return digit != null && Math.abs(digit - 0.5d) < 1e-15;
        }

      case DECIMAL:
        {
          final BigDecimal bigdec = lit.getValueAs(BigDecimal.class);
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
