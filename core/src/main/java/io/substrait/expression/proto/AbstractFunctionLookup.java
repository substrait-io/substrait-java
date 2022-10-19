package io.substrait.expression.proto;

import io.substrait.expression.FunctionLookup;
import io.substrait.function.SimpleExtension;
import java.util.Map;

public abstract class AbstractFunctionLookup implements FunctionLookup {
  protected final Map<Integer, SimpleExtension.FunctionAnchor> map;

  public AbstractFunctionLookup(Map<Integer, SimpleExtension.FunctionAnchor> map) {
    this.map = map;
  }

  public SimpleExtension.ScalarFunctionVariant getScalarFunction(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    SimpleExtension.FunctionAnchor anchor = map.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getScalarFunction(anchor);
  }

  public SimpleExtension.AggregateFunctionVariant getAggregateFunction(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    SimpleExtension.FunctionAnchor anchor = map.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getAggregateFunction(anchor);
  }
}
