package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import java.util.Locale;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Decimal arithmetic overflow options from spec v0.103.0 supported by Calcite. */
final class DecimalFunctionOptions {
  private DecimalFunctionOptions() {}

  private static SqlOperator operator(ScalarFunctionVariant function) {
    if (!DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC_DECIMAL.equals(function.urn())
        || !function.key().equals(function.name() + ":dec_dec")) return null;
    switch (function.name()) {
      case "add":
        return SqlStdOperatorTable.PLUS;
      case "subtract":
        return SqlStdOperatorTable.MINUS;
      case "multiply":
        return SqlStdOperatorTable.MULTIPLY;
      case "divide":
        return SqlStdOperatorTable.DIVIDE;
      case "modulus":
        return SqlStdOperatorTable.MOD;
      default:
        return null;
    }
  }

  static List<FunctionOption> forCall(RexCall call, ScalarFunctionVariant function) {
    SqlOperator operator = operator(function);
    if (operator == null
        || call.getOperator() != operator
        || call.getType().getSqlTypeName() != SqlTypeName.DECIMAL
        || call.getOperands().stream()
            .anyMatch(arg -> arg.getType().getSqlTypeName() != SqlTypeName.DECIMAL))
      return List.of();
    // Calcite's BigDecimal arithmetic does not enforce the declared result precision.
    return List.of(FunctionOption.builder().name("overflow").addValues("SILENT").build());
  }

  static SqlOperator resolve(Expression.ScalarFunctionInvocation expression, SqlOperator selected) {
    SqlOperator nativeOperator = operator(expression.declaration());
    if (nativeOperator == null || expression.options().isEmpty()) return selected;
    if (selected != nativeOperator)
      throw new UnsupportedOperationException(
          "No decimal option policy for Calcite operator " + selected.getName());
    // A remainder is bounded by both operands, so valid decimal inputs cannot overflow
    // the spec's modulus result type. The other operators can exceed its precision.
    List<String> supported =
        nativeOperator == SqlStdOperatorTable.MOD
            ? List.of("SILENT", "SATURATE", "ERROR")
            : List.of("SILENT");
    String previous = null;
    for (FunctionOption option : expression.options()) {
      String name = option.getName().toLowerCase(Locale.ROOT);
      if (!name.equals("overflow"))
        throw new UnsupportedOperationException("Unsupported decimal arithmetic option: " + name);
      String value =
          option.values().stream()
              .map(v -> v.toUpperCase(Locale.ROOT))
              .filter(supported::contains)
              .findFirst()
              .orElseThrow(
                  () ->
                      new UnsupportedOperationException(
                          "Unsupported decimal arithmetic overflow preferences: "
                              + option.values()));
      if (previous != null && !previous.equals(value))
        throw new UnsupportedOperationException("Conflicting decimal arithmetic option: overflow");
      previous = value;
    }
    return selected;
  }
}
