package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/** The spec v0.103.0 string options supported by the corresponding Calcite operators. */
final class StringFunctionOptions implements ScalarFunctionOptionPolicy {
  private static final Map<String, Binding> BINDINGS =
      Map.of(
          "concat",
              new Binding(
                  "null_handling",
                  "ACCEPT_NULLS",
                  Set.of(SqlStdOperatorTable.CONCAT, SqlLibraryOperators.CONCAT_FUNCTION)),
          "like",
              new Binding("case_sensitivity", "CASE_SENSITIVE", Set.of(SqlStdOperatorTable.LIKE)),
          "replace",
              new Binding(
                  "case_sensitivity", "CASE_SENSITIVE", Set.of(SqlStdOperatorTable.REPLACE)),
          "starts_with",
              new Binding(
                  "case_sensitivity", "CASE_SENSITIVE", Set.of(SqlLibraryOperators.STARTS_WITH)),
          "ends_with",
              new Binding(
                  "case_sensitivity", "CASE_SENSITIVE", Set.of(SqlLibraryOperators.ENDS_WITH)),
          "strpos",
              new Binding(
                  "case_sensitivity", "CASE_SENSITIVE", Set.of(SqlStdOperatorTable.POSITION)),
          "substring",
              new Binding(
                  "negative_start", "LEFT_OF_BEGINNING", Set.of(SqlStdOperatorTable.SUBSTRING)),
          "lower", new Binding("char_set", "UTF8", Set.of(SqlStdOperatorTable.LOWER)),
          "upper", new Binding("char_set", "UTF8", Set.of(SqlStdOperatorTable.UPPER)),
          "initcap", new Binding("char_set", null, Set.of(SqlStdOperatorTable.INITCAP)));

  @Override
  public List<FunctionOption> forCall(RexCall call, ScalarFunctionVariant function) {
    Binding binding = binding(function);
    if (binding == null
        || binding.value() == null
        || !binding.operators().contains(call.getOperator())) {
      return List.of();
    }
    // An omitted option lets a consumer choose any supported behavior. Emit the one
    // the producer's Calcite operator actually implements instead of a YAML default.
    return List.of(
        FunctionOption.builder().name(binding.name()).addValues(binding.value()).build());
  }

  @Override
  public SqlOperator resolve(Expression.ScalarFunctionInvocation expression, SqlOperator operator) {
    Binding binding = binding(expression.declaration());
    if (binding == null) {
      return operator;
    }
    if (!expression.options().isEmpty() && !binding.operators().contains(operator)) {
      throw new UnsupportedOperationException(
          "No string option policy for Calcite operator " + operator.getName());
    }
    for (FunctionOption option : expression.options()) {
      if (!binding.name().equalsIgnoreCase(option.getName())) {
        throw new UnsupportedOperationException(
            "Unsupported " + expression.declaration().name() + " option: " + option.getName());
      }
      ScalarFunctionOptionPolicy.requireDeclaredValues(expression, option);
      if (binding.value() == null) {
        // The spec lists initcap's charsets without defining ASCII word boundaries.
        throw new UnsupportedOperationException(
            "No established Calcite initcap charset option policy");
      }
      // Preferences name acceptable behaviors, in order. These operators implement one
      // behavior each, so skip other preferences and reject when none is supported.
      if (option.values().stream().noneMatch(binding.value()::equalsIgnoreCase)) {
        throw new UnsupportedOperationException(
            "Calcite "
                + expression.declaration().name()
                + " requires "
                + binding.name()
                + " "
                + binding.value()
                + "; preferences: "
                + option.values());
      }
    }
    return operator;
  }

  private static Binding binding(ScalarFunctionVariant function) {
    return DefaultExtensionCatalog.FUNCTIONS_STRING.equals(function.urn())
        ? BINDINGS.get(function.name())
        : null;
  }

  private static final class Binding {
    private final String name;
    private final String value;
    private final Set<SqlOperator> operators;

    private Binding(String name, String value, Set<SqlOperator> operators) {
      this.name = name;
      this.value = value;
      this.operators = operators;
    }

    private String name() {
      return name;
    }

    private String value() {
      return value;
    }

    private Set<SqlOperator> operators() {
      return operators;
    }
  }
}
