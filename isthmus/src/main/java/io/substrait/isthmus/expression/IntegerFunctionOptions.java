package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** Signed integer arithmetic options from spec v0.103.0 supported by Calcite. */
final class IntegerFunctionOptions {
  private IntegerFunctionOptions() {}

  static SqlOperator unchecked(SqlOperator operator) {
    if (operator == SqlStdOperatorTable.CHECKED_PLUS) return SqlStdOperatorTable.PLUS;
    if (operator == SqlStdOperatorTable.CHECKED_MINUS) return SqlStdOperatorTable.MINUS;
    if (operator == SqlStdOperatorTable.CHECKED_MULTIPLY) return SqlStdOperatorTable.MULTIPLY;
    if (operator == SqlStdOperatorTable.CHECKED_DIVIDE) return SqlStdOperatorTable.DIVIDE;
    return operator;
  }

  static boolean integerCall(RexCall call) {
    switch (call.getType().getSqlTypeName()) {
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return call.getOperands().stream()
            .allMatch(arg -> arg.getType().getSqlTypeName() == call.getType().getSqlTypeName());
      default:
        return false;
    }
  }

  private static Binding binding(ScalarFunctionVariant function) {
    if (!DefaultExtensionCatalog.FUNCTIONS_ARITHMETIC.equals(function.urn())) return null;
    String name = function.name();
    for (String type : List.of("i8", "i16", "i32", "i64")) {
      if (!function.key().equals(name + ":" + type + "_" + type)) continue;
      boolean narrow = type.equals("i8") || type.equals("i16");
      switch (name) {
        case "add":
          return new Binding(
              SqlStdOperatorTable.PLUS, SqlStdOperatorTable.CHECKED_PLUS, narrow, false, false);
        case "subtract":
          return new Binding(
              SqlStdOperatorTable.MINUS, SqlStdOperatorTable.CHECKED_MINUS, narrow, false, false);
        case "multiply":
          return new Binding(
              SqlStdOperatorTable.MULTIPLY,
              SqlStdOperatorTable.CHECKED_MULTIPLY,
              narrow,
              false,
              false);
        case "divide":
          return new Binding(
              SqlStdOperatorTable.DIVIDE, SqlStdOperatorTable.CHECKED_DIVIDE, narrow, true, false);
        case "modulus":
          return new Binding(SqlStdOperatorTable.MOD, SqlStdOperatorTable.MOD, false, false, true);
        default:
          return null;
      }
    }
    return null;
  }

  static List<FunctionOption> forCall(RexCall call, ScalarFunctionVariant function) {
    Binding binding = binding(function);
    if (binding == null
        || !integerCall(call)
        || (call.getOperator() != binding.normal && call.getOperator() != binding.checked))
      return List.of();
    String overflow =
        binding.narrow || (call.getOperator() == binding.checked && !binding.modulus)
            ? "ERROR"
            : "SILENT";
    List<FunctionOption> options = new ArrayList<>();
    options.add(option("overflow", overflow));
    if (binding.divide) {
      options.add(option("on_domain_error", "ERROR"));
      options.add(option("on_division_by_zero", "ERROR"));
    }
    if (binding.modulus) {
      options.add(option("division_type", "TRUNCATE"));
      options.add(option("on_domain_error", "ERROR"));
    }
    return options;
  }

  private static FunctionOption option(String name, String value) {
    return FunctionOption.builder().name(name).addValues(value).build();
  }

  static SqlOperator resolve(Expression.ScalarFunctionInvocation expression, SqlOperator operator) {
    Binding binding = binding(expression.declaration());
    if (binding == null) return operator;
    if (operator != binding.normal && operator != binding.checked) {
      if (!expression.options().isEmpty())
        throw new UnsupportedOperationException(
            "No integer option policy for Calcite operator " + operator.getName());
      return operator;
    }
    Map<String, String> selected = new LinkedHashMap<>();
    for (FunctionOption option : expression.options()) {
      String name = option.getName().toLowerCase(java.util.Locale.ROOT);
      List<String> supported;
      if (name.equals("overflow")) {
        supported = binding.narrow ? List.of("ERROR") : List.of("SILENT", "ERROR");
      } else if ((binding.divide
              && (name.equals("on_domain_error") || name.equals("on_division_by_zero")))
          || (binding.modulus && name.equals("on_domain_error"))) {
        supported = List.of("ERROR");
      } else if (binding.modulus && name.equals("division_type")) {
        supported = List.of("TRUNCATE");
      } else {
        throw new UnsupportedOperationException("Unsupported integer arithmetic option: " + name);
      }
      String value =
          option.values().stream()
              .map(v -> v.toUpperCase(java.util.Locale.ROOT))
              .filter(supported::contains)
              .findFirst()
              .orElseThrow(
                  () ->
                      new UnsupportedOperationException(
                          "Unsupported integer arithmetic "
                              + name
                              + " preferences: "
                              + option.values()));
      String previous = selected.putIfAbsent(name, value);
      if (previous != null && !previous.equals(value))
        throw new UnsupportedOperationException("Conflicting integer arithmetic option: " + name);
    }
    String overflow = selected.get("overflow");
    if (overflow == null) return operator;
    return overflow.equals("ERROR") ? binding.checked : binding.normal;
  }

  private static final class Binding {
    private final SqlOperator normal;
    private final SqlOperator checked;
    private final boolean narrow;
    private final boolean divide;
    private final boolean modulus;

    private Binding(
        SqlOperator normal, SqlOperator checked, boolean narrow, boolean divide, boolean modulus) {
      this.normal = normal;
      this.checked = checked;
      this.narrow = narrow;
      this.divide = divide;
      this.modulus = modulus;
    }
  }
}
