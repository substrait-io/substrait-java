package io.substrait.expression;

import io.substrait.function.SimpleExtension;

/**
 * Interface with operations for resolving references to {@link
 * io.substrait.proto.SimpleExtensionDeclaration}s within an individual plan to their corresponding
 * functions or types.
 */
public interface ExtensionLookup {
  // TODO: Move to io.substrait.extension
  SimpleExtension.ScalarFunctionVariant getScalarFunction(
      int reference, SimpleExtension.ExtensionCollection extensions);

  SimpleExtension.AggregateFunctionVariant getAggregateFunction(
      int reference, SimpleExtension.ExtensionCollection extensions);

  SimpleExtension.Type getType(int reference, SimpleExtension.ExtensionCollection extensions);
}
