package io.substrait.type;

import io.substrait.extension.SimpleExtension;
import io.substrait.function.TypeExpression;
import java.util.List;

/**
 * Evaluates a {@link TypeExpression} to a concrete {@link Type} given a set of actual arguments.
 */
public class TypeExpressionEvaluator {

  /**
   * Evaluates a return-type expression to a concrete {@link Type}.
   *
   * @param returnExpression the type expression to evaluate
   * @param parameterizedTypeList the declared parameter types of the function
   * @param actualTypes the actual argument types supplied at the call site
   * @return the resolved concrete type
   * @throws UnsupportedOperationException if the expression cannot yet be evaluated
   */
  public static Type evaluateExpression(
      TypeExpression returnExpression,
      List<SimpleExtension.Argument> parameterizedTypeList,
      List<Type> actualTypes) {

    if (returnExpression instanceof Type) {
      return (Type) returnExpression;
    }
    throw new UnsupportedOperationException("NYI");
  }
}
