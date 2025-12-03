package io.substrait.extension;

import java.util.Map;

public abstract class AbstractExtensionLookup implements ExtensionLookup {
  protected final Map<Integer, SimpleExtension.FunctionAnchor> functionAnchorMap;
  protected final Map<Integer, SimpleExtension.TypeAnchor> typeAnchorMap;

  public AbstractExtensionLookup(
      final Map<Integer, SimpleExtension.FunctionAnchor> functionAnchorMap,
      final Map<Integer, SimpleExtension.TypeAnchor> typeAnchorMap) {
    this.functionAnchorMap = functionAnchorMap;
    this.typeAnchorMap = typeAnchorMap;
  }

  @Override
  public SimpleExtension.ScalarFunctionVariant getScalarFunction(
      final int reference, final SimpleExtension.ExtensionCollection extensions) {
    final SimpleExtension.FunctionAnchor anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getScalarFunction(anchor);
  }

  @Override
  public SimpleExtension.WindowFunctionVariant getWindowFunction(
      final int reference, final SimpleExtension.ExtensionCollection extensions) {
    final SimpleExtension.FunctionAnchor anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getWindowFunction(anchor);
  }

  @Override
  public SimpleExtension.AggregateFunctionVariant getAggregateFunction(
      final int reference, final SimpleExtension.ExtensionCollection extensions) {
    final SimpleExtension.FunctionAnchor anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getAggregateFunction(anchor);
  }

  @Override
  public SimpleExtension.Type getType(
      final int reference, final SimpleExtension.ExtensionCollection extensions) {
    final SimpleExtension.TypeAnchor anchor = typeAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown type id. Make sure that the type id provided was shared in the extensions section of the plan.");
    }

    return extensions.getType(anchor);
  }
}
