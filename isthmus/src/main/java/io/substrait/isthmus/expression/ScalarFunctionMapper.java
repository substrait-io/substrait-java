package io.substrait.isthmus.expression;

import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rex.RexCall;

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
   * If the supplied Substrait expression is applicable to this mapper, get the function arguments
   * that should be used when mapping to the corresponding Calcite function.
   *
   * @param expression an expression.
   * @return a list of function arguments, or an empty Optional if no mapping exists.
   */
  Optional<List<FunctionArg>> getExpressionArguments(
      Expression.ScalarFunctionInvocation expression);
}
