package io.substrait.extension;

import java.util.Map;

/**
 * Abstract base class for {@link ExtensionLookup} implementations that use maps to resolve
 * extension references to their corresponding function and type anchors.
 */
public abstract class AbstractExtensionLookup implements ExtensionLookup {
  /** Map of function reference IDs to their corresponding function anchors. */
  protected final Map<Integer, SimpleExtension.FunctionAnchor> functionAnchorMap;

  /** Map of type reference IDs to their corresponding type anchors. */
  protected final Map<Integer, SimpleExtension.TypeAnchor> typeAnchorMap;

  /**
   * Constructs an AbstractExtensionLookup with the provided anchor maps.
   *
   * @param functionAnchorMap map of function reference IDs to function anchors
   * @param typeAnchorMap map of type reference IDs to type anchors
   */
  public AbstractExtensionLookup(
      Map<Integer, SimpleExtension.FunctionAnchor> functionAnchorMap,
      Map<Integer, SimpleExtension.TypeAnchor> typeAnchorMap) {
    this.functionAnchorMap = functionAnchorMap;
    this.typeAnchorMap = typeAnchorMap;
  }

  @Override
  public SimpleExtension.ScalarFunctionVariant getScalarFunction(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    SimpleExtension.FunctionAnchor anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getScalarFunction(anchor);
  }

  @Override
  public SimpleExtension.WindowFunctionVariant getWindowFunction(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    SimpleExtension.FunctionAnchor anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getWindowFunction(anchor);
  }

  @Override
  public SimpleExtension.AggregateFunctionVariant getAggregateFunction(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    SimpleExtension.FunctionAnchor anchor = functionAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown function id. Make sure that the function id provided was shared in the extensions section of the plan.");
    }

    return extensions.getAggregateFunction(anchor);
  }

  @Override
  public SimpleExtension.Type getType(
      int reference, SimpleExtension.ExtensionCollection extensions) {
    SimpleExtension.TypeAnchor anchor = typeAnchorMap.get(reference);
    if (anchor == null) {
      throw new IllegalArgumentException(
          "Unknown type id. Make sure that the type id provided was shared in the extensions section of the plan.");
    }

    return extensions.getType(anchor);
  }
}
