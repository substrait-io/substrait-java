package io.substrait.expression;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;

/**
 * Helper class for validating variadic parameter consistency in function invocations. Validates that
 * when parameterConsistency is CONSISTENT, all variadic arguments have the same type (ignoring
 * nullability).
 */
public class VariadicParameterConsistencyValidator {

  /**
   * Validates that variadic arguments satisfy the parameter consistency requirement. When
   * CONSISTENT, all variadic arguments must have the same type (ignoring nullability). When
   * INCONSISTENT, arguments can have different types.
   *
   * @param func the function declaration
   * @param arguments the function arguments to validate
   * @throws AssertionError if validation fails
   */
  public static void validate(
      SimpleExtension.Function func, List<FunctionArg> arguments) {
    java.util.Optional<SimpleExtension.VariadicBehavior> variadic = func.variadic();
    if (!variadic.isPresent()) {
      return;
    }

    SimpleExtension.VariadicBehavior variadicBehavior = variadic.get();
    if (variadicBehavior.parameterConsistency()
        != SimpleExtension.VariadicBehavior.ParameterConsistency.CONSISTENT) {
      // INCONSISTENT allows different types, so validation passes
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
            .collect(java.util.stream.Collectors.toList());

    int fixedArgCount = func.args().size();
    if (argumentTypes.size() <= fixedArgCount) {
      // No variadic arguments, validation passes
      return;
    }

    // For CONSISTENT, all variadic arguments must have the same type (ignoring nullability)
    // Compare all variadic arguments to the first one for more informative error messages
    // Variadic arguments start after the fixed arguments
    int firstVariadicArgIdx = fixedArgCount + Math.max(variadicBehavior.getMin() - 1, 0);
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

