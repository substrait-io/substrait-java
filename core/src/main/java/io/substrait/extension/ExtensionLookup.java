package io.substrait.extension;

/**
 * Interface with operations for resolving references to {@link
 * io.substrait.proto.SimpleExtensionDeclaration}s within an individual plan to their corresponding
 * functions or types.
 */
public interface ExtensionLookup {
  /**
   * Resolves a scalar function reference to its corresponding function variant.
   *
   * @param reference the function reference ID
   * @param extensions the extension collection to search
   * @return the scalar function variant
   */
  SimpleExtension.ScalarFunctionVariant getScalarFunction(
      int reference, SimpleExtension.ExtensionCollection extensions);

  /**
   * Resolves a window function reference to its corresponding function variant.
   *
   * @param reference the function reference ID
   * @param extensions the extension collection to search
   * @return the window function variant
   */
  SimpleExtension.WindowFunctionVariant getWindowFunction(
      int reference, SimpleExtension.ExtensionCollection extensions);

  /**
   * Resolves an aggregate function reference to its corresponding function variant.
   *
   * @param reference the function reference ID
   * @param extensions the extension collection to search
   * @return the aggregate function variant
   */
  SimpleExtension.AggregateFunctionVariant getAggregateFunction(
      int reference, SimpleExtension.ExtensionCollection extensions);

  /**
   * Resolves a type reference to its corresponding type.
   *
   * @param reference the type reference ID
   * @param extensions the extension collection to search
   * @return the type
   */
  SimpleExtension.Type getType(int reference, SimpleExtension.ExtensionCollection extensions);
}
