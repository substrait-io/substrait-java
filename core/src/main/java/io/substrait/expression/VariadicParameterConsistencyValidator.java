package io.substrait.expression;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Helper class for validating variadic parameter consistency in function invocations. Validates
 * that when parameterConsistency is CONSISTENT, all variadic arguments have the same type (ignoring
 * nullability).
 */
class VariadicParameterConsistencyValidator {

  /**
   * Validates that variadic arguments satisfy the parameter consistency requirement. When
   * CONSISTENT, all variadic arguments must have the same type (ignoring nullability). When
   * INCONSISTENT, arguments can have any type satisfying the type constraints
   *
   * @param func the function declaration
   * @param arguments the function arguments to validate
   * @throws AssertionError if validation fails
   */
  public static void validate(SimpleExtension.Function func, List<FunctionArg> arguments) {
    Optional<SimpleExtension.VariadicBehavior> variadic = func.variadic();
    if (!variadic.isPresent()) {
      return;
    }

    SimpleExtension.VariadicBehavior variadicBehavior = variadic.get();
    if (variadicBehavior.parameterConsistency()
        != SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT) {
      // INCONSISTENT allows different types, so validation passes
      // TODO (#633): Even when parameterConsistency is INCONSISTENT, there can be implicit
      // constraints
      // across variadic parameters due to type parameters. For example, consider a function with:
      //   args: [value: "decimal<P,S>", variadic: {min: 1, parameterConsistency: INCONSISTENT}]
      //   return: "decimal<38,S>"
      // In this case, while the precision P can vary across variadic arguments, the scale S must
      // be consistent across all variadic arguments (since it's used in the return type). The
      // current implementation doesn't validate these type parameter constraints. According to
      // the spec: "Each argument can be any possible concrete type afforded by the bounds of any
      // parameter defined in the arguments specification." This means we need to check that type
      // parameters that appear in the return type (or are otherwise constrained) are consistent
      // across variadic arguments, even when parameterConsistency is INCONSISTENT.
      return;
    }

    // Extract types from arguments (only Expression and Type have types, EnumArg doesn't)
    List<Type> argumentTypes =
        arguments.stream()
            .filter(arg -> arg instanceof Expression || arg instanceof Type)
            .map(
                arg -> {
                  if (arg instanceof Expression) {
                    return ((Expression) arg).getType();
                  } else {
                    return (Type) arg;
                  }
                })
            .collect(Collectors.toList());

    // Count how many Expression/Type arguments are in the fixed arguments (before variadic)
    // Note: func.args() includes all argument types (Expression, Type, EnumArg), but we only
    // care about Expression/Type arguments for type consistency checking
    int nonVariadicArgCount = 0;
    for (int i = 0; i < func.args().size() && i < arguments.size(); i++) {
      FunctionArg arg = arguments.get(i);
      if (arg instanceof Expression || arg instanceof Type) {
        nonVariadicArgCount++;
      }
    }

    if (argumentTypes.size() <= nonVariadicArgCount) {
      // No variadic arguments, validation passes
      return;
    }

    // For CONSISTENT, all variadic arguments must have the same type (ignoring nullability)
    // Compare all variadic arguments to the first one for more informative error messages
    // Variadic arguments start immediately after the fixed arguments
    int firstVariadicArgIdx = nonVariadicArgCount;
    Type firstVariadicType = argumentTypes.get(firstVariadicArgIdx);
    for (int i = firstVariadicArgIdx + 1; i < argumentTypes.size(); i++) {
      Type currentType = argumentTypes.get(i);
      if (!firstVariadicType.equalsIgnoringNullability(currentType)) {
        throw new AssertionError(
            String.format(
                "Variadic arguments must have consistent types when parameterConsistency is CONSISTENT. "
                    + "Argument at index %d has type %s but argument at index %d has type %s",
                firstVariadicArgIdx, firstVariadicType, i, currentType));
      }
    }
  }
}
