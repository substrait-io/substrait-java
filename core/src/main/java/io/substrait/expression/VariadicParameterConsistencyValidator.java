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
    int firstVariadicArgIdx = Math.max(variadicBehavior.getMin() - 1, 0);
    for (int i = firstVariadicArgIdx; i < argumentTypes.size() - 1; i++) {
      Type currentType = argumentTypes.get(i);
      Type nextType = argumentTypes.get(i + 1);
      // Normalize both types to nullable for comparison (ignoring nullability)
      if (!io.substrait.type.TypeCreator.asNullable(currentType)
          .equals(io.substrait.type.TypeCreator.asNullable(nextType))) {
        throw new AssertionError(
            String.format(
                "Variadic arguments must have consistent types when parameterConsistency is CONSISTENT. "
                    + "Argument at index %d has type %s but argument at index %d has type %s",
                i, currentType, i + 1, nextType));
      }
    }
  }
}

