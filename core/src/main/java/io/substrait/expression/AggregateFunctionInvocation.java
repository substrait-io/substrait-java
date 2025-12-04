package io.substrait.expression;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class AggregateFunctionInvocation {
  public abstract SimpleExtension.AggregateFunctionVariant declaration();

  public abstract List<FunctionArg> arguments();

  public abstract List<FunctionOption> options();

  public abstract Expression.AggregationPhase aggregationPhase();

  public abstract List<Expression.SortField> sort();

  public abstract Type outputType();

  public Type getType() {
    return outputType();
  }

  public abstract Expression.AggregationInvocation invocation();

  /**
   * Validates that variadic arguments satisfy the parameter consistency requirement. When
   * CONSISTENT, all variadic arguments must have the same type (ignoring nullability). When
   * INCONSISTENT, arguments can have different types.
   */
  @Value.Check
  protected void check() {
    SimpleExtension.Function func = declaration();
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
        arguments().stream()
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
      assert io.substrait.type.TypeCreator.asNullable(currentType)
              .equals(io.substrait.type.TypeCreator.asNullable(nextType))
          : String.format(
              "Variadic arguments must have consistent types when parameterConsistency is CONSISTENT. "
                  + "Argument at index %d has type %s but argument at index %d has type %s",
              i, currentType, i + 1, nextType);
    }
  }

  public static ImmutableAggregateFunctionInvocation.Builder builder() {
    return ImmutableAggregateFunctionInvocation.builder();
  }
}
