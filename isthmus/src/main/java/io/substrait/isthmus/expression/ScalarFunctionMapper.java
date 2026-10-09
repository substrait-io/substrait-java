package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/**
 * Provides custom conversion between a Calcite call and corresponding Substrait functions and
 * arguments.
 */
interface ScalarFunctionMapper {

  /**
   * If the supplied Calcite call is applicable to this mapper, get the custom mapping to the
   * corresponding Substrait function.
   *
   * @param call a Calcite call.
   * @return a custom function mapping, or an empty Optional if no mapping exists.
   */
  Optional<SubstraitFunctionMapping> toSubstrait(RexCall call);

  /**
   * Builds a custom Calcite expression when the selected operator needs a reverse mapping.
   *
   * @param expression the Substrait invocation
   * @param operator the selected Calcite operator
   * @param arguments converted arguments
   * @param returnType the declared result type
   * @param rexBuilder builder for the target Calcite plan
   * @return the custom expression, or empty to use the selected operator directly
   */
  default Optional<RexNode> toCalcite(
      Expression.ScalarFunctionInvocation expression,
      SqlOperator operator,
      List<RexNode> arguments,
      RelDataType returnType,
      RexBuilder rexBuilder) {
    return Optional.empty();
  }

  /**
   * If the supplied Substrait expression is applicable to this mapper, get the function arguments
   * that should be used when mapping to the corresponding Calcite function.
   *
   * @param expression an expression.
   * @return a list of function arguments, or an empty Optional if no mapping exists.
   */
  Optional<List<FunctionArg>> getExpressionArguments(
      Expression.ScalarFunctionInvocation expression);
}
