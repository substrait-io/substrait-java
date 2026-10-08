package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * Maps the SQL {@code ||} binary concatenation operator ({@link SqlStdOperatorTable#CONCAT}) to the
 * Substrait {@code concat} function. This allows {@code ||} to continue working in the Calcite to
 * Substrait direction while {@link org.apache.calcite.sql.fun.SqlLibraryOperators#CONCAT_FUNCTION}
 * serves as the canonical Substrait-Calcite mapping via {@link FunctionMappings#SCALAR_SIGS}.
 */
final class ConcatFunctionMapper implements ScalarFunctionMapper {
  private static final String CONCAT_FUNCTION_NAME = "concat";
  private final List<ScalarFunctionVariant> concatFunctions;

  static List<FunctionOption> optionsFor(RexCall call, ScalarFunctionVariant function) {
    if (!isStandardConcat(function)
        || !(SqlStdOperatorTable.CONCAT.equals(call.getOperator())
            || SqlLibraryOperators.CONCAT_FUNCTION.equals(call.getOperator()))) {
      return List.of();
    }
    // Both Calcite operators propagate null. An omitted option lets a Substrait consumer
    // choose IGNORE_NULLS instead (spec v0.103.0).
    return List.of(
        FunctionOption.builder().name("null_handling").addValues("ACCEPT_NULLS").build());
  }

  private static boolean isStandardConcat(ScalarFunctionVariant function) {
    return CONCAT_FUNCTION_NAME.equals(function.name())
        && DefaultExtensionCatalog.FUNCTIONS_STRING.equals(function.urn());
  }

  ConcatFunctionMapper(List<ScalarFunctionVariant> functions) {
    this.concatFunctions =
        functions.stream()
            .filter(
                f ->
                    CONCAT_FUNCTION_NAME.equals(f.name())
                        && DefaultExtensionCatalog.FUNCTIONS_STRING.equals(f.urn()))
            .collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Optional<SubstraitFunctionMapping> toSubstrait(RexCall call) {
    if (concatFunctions.isEmpty() || !SqlStdOperatorTable.CONCAT.equals(call.getOperator())) {
      return Optional.empty();
    }
    return Optional.of(
        new SubstraitFunctionMapping(CONCAT_FUNCTION_NAME, call.getOperands(), concatFunctions));
  }

  @Override
  public Optional<List<FunctionArg>> getExpressionArguments(
      Expression.ScalarFunctionInvocation expression) {
    if (isStandardConcat(expression.declaration())) {
      for (FunctionOption option : expression.options()) {
        if (!"null_handling".equalsIgnoreCase(option.getName())) {
          throw new UnsupportedOperationException("Unsupported concat option: " + option.getName());
        }
        // FunctionOption preferences name acceptable behaviors, in order. Calcite supports
        // only ACCEPT_NULLS, so skip other preferences and reject when none is supported.
        if (option.values().stream().noneMatch("ACCEPT_NULLS"::equalsIgnoreCase)) {
          throw new UnsupportedOperationException(
              "Calcite concat requires null_handling ACCEPT_NULLS; preferences: "
                  + option.values());
        }
      }
    }
    return Optional.empty();
  }
}
