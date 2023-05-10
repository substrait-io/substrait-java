package io.substrait.expression.proto;

import io.substrait.expression.FunctionLookup;
import io.substrait.function.SimpleExtension;
import java.util.Map;

public abstract class AbstractFunctionLookup implements FunctionLookup {
  // TODO: Rename to AbstractExtensionLookup and move to io.substrait.extension
  protected final Map<Integer, SimpleExtension.FunctionAnchor> functionAnchorMap;
  protected final Map<Integer, SimpleExtension.TypeAnchor> typeAnchorMap;

  public AbstractFunctionLookup(
      Map<Integer, SimpleExtension.FunctionAnchor> functionAnchorMap,
      Map<Integer, SimpleExtension.TypeAnchor> typeAnchorMap) {
    this.functionAnchorMap = functionAnchorMap;
    this.typeAnchorMap = typeAnchorMap;
  }

  public SimpleExtension.ScalarFunctionVariant getScalarFunction(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    var anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getScalarFunction(anchor);
  }

  public SimpleExtension.AggregateFunctionVariant getAggregateFunction(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    var anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getAggregateFunction(anchor);
  }

  public SimpleExtension.Type getType(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    var anchor = typeAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown type id. Make sure that the type id provided was shared in the extensions section of the plan.");
    }

    return extensions.getType(anchor);
  }
}
