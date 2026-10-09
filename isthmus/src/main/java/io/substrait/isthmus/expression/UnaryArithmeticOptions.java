package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Unary arithmetic option policies from spec v0.103.0. */
final class UnaryArithmeticOptions implements ScalarFunctionOptionPolicy {
  private static final Set<String> NAMES =
      Set.of(
          "negate",
          "abs",
          "sqrt",
          "exp",
          "cos",
          "sin",
          "tan",
          "cosh",
          "sinh",
          "tanh",
          "acos",
          "asin",
          "atan",
          "acosh",
          "asinh",
          "atanh",
          "radians",
          "degrees",
          "factorial");

  private static String name(ScalarFunctionVariant function) {
    if (!DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC.equals(function.urn())
        || !NAMES.contains(function.name())) return null;
    for (String tag : List.of("i8", "i16", "i32", "i64", "fp32", "fp64"))
      if (function.key().equals(function.name() + ":" + tag)) return function.name();
    return null;
  }

  private static boolean integer(ScalarFunctionVariant function) {
    return function.key().matches("(?:negate|abs):i(?:8|16|32|64)");
  }

  private static SqlOperator nativeOperator(String name) {
    return FunctionMappings.SCALAR_SIGS.stream()
        .filter(sig -> sig.name().equals(name))
        .map(FunctionMappings.Sig::operator)
        .findFirst()
        .orElseThrow();
  }

  @Override
  public SqlOperator signatureOperator(RexCall call) {
    return checkedIntegerNegation(call) ? SqlStdOperatorTable.UNARY_MINUS : call.getOperator();
  }

  static boolean checkedIntegerNegation(RexCall call) {
    SqlTypeName type = call.getType().getSqlTypeName();
    return call.getOperator() == SqlStdOperatorTable.CHECKED_UNARY_MINUS
        && call.getOperands().size() == 1
        && call.getOperands().get(0).getType().getSqlTypeName() == type
        && (type == SqlTypeName.TINYINT
            || type == SqlTypeName.SMALLINT
            || type == SqlTypeName.INTEGER
            || type == SqlTypeName.BIGINT);
  }

  private static FunctionOption option(String name, String value) {
    return FunctionOption.builder().name(name).addValues(value).build();
  }

  @Override
  public List<FunctionOption> forCall(RexCall call, ScalarFunctionVariant function) {
    String name = name(function);
    if (name == null || call.getOperands().size() != 1) return List.of();
    if (name.equals("negate") && integer(function) && checkedIntegerNegation(call))
      return List.of(option("overflow", "ERROR"));
    if (call.getOperator() != nativeOperator(name)) return List.of();
    if ((name.equals("negate") || name.equals("abs")) && integer(function))
      return List.of(option("overflow", "SILENT"));
    // Calcite's FP32 unary conversion path cannot carry a NaN result.
    if ((name.equals("acos") || name.equals("asin")) && function.key().endsWith(":fp64"))
      return List.of(option("on_domain_error", "NAN"));
    // Java's transcendental functions need not be correctly rounded. SQL SQRT is
    // represented as POWER(x, 0.5), without a verified explicit option policy.
    return List.of();
  }

  @Override
  public SqlOperator resolve(Expression.ScalarFunctionInvocation expression, SqlOperator selected) {
    String name = name(expression.declaration());
    if (name == null || expression.options().isEmpty()) return selected;
    SqlOperator nativeOperator = nativeOperator(name);
    boolean negate = name.equals("negate") && integer(expression.declaration());
    if (selected != nativeOperator
        && !(negate && selected == SqlStdOperatorTable.CHECKED_UNARY_MINUS))
      throw new UnsupportedOperationException(
          "No unary arithmetic option policy for Calcite operator " + selected.getName());
    String overflow = null;
    for (FunctionOption option : expression.options()) {
      String optionName = option.getName().toLowerCase(Locale.ROOT);
      List<String> supported;
      if (optionName.equals("overflow") && integer(expression.declaration()))
        supported = negate ? List.of("SILENT", "ERROR") : List.of("SILENT");
      else if (optionName.equals("on_domain_error")
          && (name.equals("acos") || name.equals("asin"))
          && expression.declaration().key().endsWith(":fp64")) supported = List.of("NAN");
      else supported = List.of();
      String value =
          option.values().stream()
              .map(v -> v.toUpperCase(Locale.ROOT))
              .filter(supported::contains)
              .findFirst()
              .orElseThrow(
                  () ->
                      new UnsupportedOperationException(
                          "Unsupported unary arithmetic "
                              + name
                              + " "
                              + optionName
                              + " preferences: "
                              + option.values()));
      if (optionName.equals("overflow")) {
        if (overflow != null && !overflow.equals(value))
          throw new UnsupportedOperationException("Conflicting unary arithmetic option: overflow");
        overflow = value;
      }
    }
    if (!negate || overflow == null) return selected;
    return overflow.equals("ERROR")
        ? SqlStdOperatorTable.CHECKED_UNARY_MINUS
        : SqlStdOperatorTable.UNARY_MINUS;
  }
}
