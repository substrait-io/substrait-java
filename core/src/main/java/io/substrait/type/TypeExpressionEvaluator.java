package io.substrait.type;

import io.substrait.extension.SimpleExtension;
import io.substrait.function.TypeExpression;
import java.util.List;

public class TypeExpressionEvaluator {

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
