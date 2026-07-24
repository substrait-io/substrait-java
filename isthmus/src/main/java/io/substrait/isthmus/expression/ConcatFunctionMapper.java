package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.DefaultExtensionCatalog;
import io.substrait.extension.SimpleExtension.ScalarFunctionVariant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rex.RexCall;
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
    return Optional.empty();
  }
}
