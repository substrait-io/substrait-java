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

/** Floating-point arithmetic options supported by Calcite. */
final class FloatingPointFunctionOptions implements ScalarFunctionOptionPolicy {

  private static SqlOperator operator(ScalarFunctionVariant function) {
    if (!DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC.equals(function.urn())) {
      return null;
    }
    String name = function.name();
    if (!function.key().equals(name + ":fp32_fp32")
        && !function.key().equals(name + ":fp64_fp64")) {
      return null;
    }
    switch (name) {
      case "add":
        return SqlStdOperatorTable.PLUS;
      case "subtract":
        return SqlStdOperatorTable.MINUS;
      case "multiply":
        return SqlStdOperatorTable.MULTIPLY;
      case "divide":
        return SqlStdOperatorTable.DIVIDE;
      default:
        return null;
    }
  }

  @Override
  public List<FunctionOption> forCall(RexCall call, ScalarFunctionVariant function) {
    SqlOperator operator = operator(function);
    if (operator == null || call.getOperator() != operator) {
      return List.of();
    }
    if (operator == SqlStdOperatorTable.DIVIDE) {
      return List.of(option("rounding", "TIE_TO_EVEN"), option("on_domain_error", "NAN"));
    }
    return List.of(option("rounding", "TIE_TO_EVEN"));
  }

  private static FunctionOption option(String name, String value) {
    return FunctionOption.builder().name(name).addValues(value).build();
  }

  @Override
  public SqlOperator resolve(Expression.ScalarFunctionInvocation expression, SqlOperator selected) {
    SqlOperator nativeOperator = operator(expression.declaration());
    if (nativeOperator == null || expression.options().isEmpty()) {
      return selected;
    }
    if (selected != nativeOperator) {
      throw new UnsupportedOperationException(
          "No floating-point option policy for Calcite operator " + selected.getName());
    }
    for (FunctionOption option : expression.options()) {
      ScalarFunctionOptionPolicy.requireDeclaredValues(expression, option);
      String name = option.getName().toLowerCase(Locale.ROOT);
      String supported;
      if (name.equals("rounding")) {
        supported = "TIE_TO_EVEN";
      } else if (nativeOperator == SqlStdOperatorTable.DIVIDE && name.equals("on_domain_error")) {
        supported = "NAN";
      } else if (nativeOperator == SqlStdOperatorTable.DIVIDE
          && name.equals("on_division_by_zero")) {
        // The IEEE option's description contradicts IEEE 754 for
        // finite nonzero dividends. Neither that text nor LIMIT describes Java division.
        throw new UnsupportedOperationException(
            "Floating-point division-by-zero options cannot be honored unambiguously");
      } else {
        throw new UnsupportedOperationException(
            "Unsupported floating-point arithmetic option: " + name);
      }
      if (option.values().stream()
          .map(v -> v.toUpperCase(Locale.ROOT))
          .noneMatch(supported::equals)) {
        throw new UnsupportedOperationException(
            "Unsupported floating-point arithmetic " + name + " preferences: " + option.values());
      }
    }
    return selected;
  }
}
