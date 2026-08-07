package io.substrait.extension;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.EnumArg;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A fully-resolved binding of an aggregate function invocation.
 *
 * <p>Wraps a {@link ResolvedFunctionBinding} with the aggregate-specific phase and invocation
 * semantics. The output type is derived from the function declaration (never from a plan-supplied
 * value) and depends on the phase, so it does not participate in the binding's identity.
 */
@Value.Immutable
public abstract class ResolvedAggregateBinding {

  /**
   * Returns the resolved function binding.
   *
   * @return the function binding
   */
  public abstract ResolvedFunctionBinding function();

  /**
   * Returns the aggregation phase.
   *
   * @return the aggregation phase
   */
  public abstract Expression.AggregationPhase phase();

  /**
   * Returns the aggregation invocation semantics (all vs. distinct).
   *
   * @return the aggregation invocation
   */
  public abstract Expression.AggregationInvocation invocation();

  /**
   * Returns the intermediate type, when it has been resolved explicitly. When empty, {@link
   * #outputType()} derives it from the declaration on demand. Not part of the binding identity.
   *
   * @return the intermediate type, if known
   */
  @Value.Auxiliary
  public abstract Optional<Type> intermediateType();

  /**
   * Returns the type this aggregate produces <em>in its phase</em>: a phase that stops at the
   * intermediate state produces the declaration's intermediate type, while a phase that runs to the
   * result produces its return type. Derived from the declaration, never from a plan-supplied
   * value.
   *
   * @return the output type of this phase
   * @throws InvalidFunctionBindingException if an intermediate type is required but the declaration
   *     is not an aggregate variant, or its type expression cannot be derived
   */
  public Type outputType() {
    return producesIntermediateState()
        ? intermediateType().orElseGet(this::deriveIntermediateType)
        : function().outputType();
  }

  /**
   * Returns whether this phase stops at the declaration's intermediate state instead of producing
   * its result.
   *
   * @return {@code true} for the initial-to-intermediate and intermediate-to-intermediate phases
   */
  public boolean producesIntermediateState() {
    return phase() == Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE
        || phase() == Expression.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE;
  }

  /**
   * Returns whether this phase consumes the declaration's intermediate state instead of its
   * declared arguments. The arguments of such an invocation are intermediate values, so they are
   * not expected to match the declaration's argument list.
   *
   * @return {@code true} for the intermediate-to-intermediate and intermediate-to-result phases
   */
  public boolean consumesIntermediateState() {
    return phase() == Expression.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE
        || phase() == Expression.AggregationPhase.INTERMEDIATE_TO_RESULT;
  }

  private Type deriveIntermediateType() {
    SimpleExtension.Function declaration = function().declaration();
    if (!(declaration instanceof SimpleExtension.AggregateFunctionVariant)) {
      throw new InvalidFunctionBindingException(
          String.format(
              "%s is not an aggregate declaration and has no intermediate type",
              function().anchor()));
    }
    return FunctionBindingResolver.deriveIntermediateType(
        (SimpleExtension.AggregateFunctionVariant) declaration, function().arguments());
  }

  /**
   * Creates a builder for {@link ResolvedAggregateBinding}.
   *
   * @return a new builder
   */
  public static ImmutableResolvedAggregateBinding.Builder builder() {
    return ImmutableResolvedAggregateBinding.builder();
  }

  /**
   * Resolves an aggregate function invocation into a binding, capturing its semantic identity
   * (anchor, arguments and options). This does <em>not</em> validate the invocation against the
   * declaration; signature, options and output-type validation is a separate, opt-in step (see
   * {@link FunctionBindingResolver#validate}). The output type is derived on demand via {@link
   * #outputType()}.
   *
   * @param invocation the aggregate invocation to resolve
   * @return the resolved aggregate binding
   */
  public static ResolvedAggregateBinding resolve(AggregateFunctionInvocation invocation) {
    List<ResolvedArgument> arguments = resolvedArguments(invocation.arguments());
    ResolvedFunctionBinding function =
        FunctionBindingResolver.resolve(invocation.declaration(), arguments, invocation.options());
    return builder()
        .function(function)
        .phase(invocation.aggregationPhase())
        .invocation(invocation.invocation())
        .intermediateType(Optional.empty())
        .build();
  }

  private static List<ResolvedArgument> resolvedArguments(List<FunctionArg> arguments) {
    List<ResolvedArgument> resolved = new ArrayList<>();
    for (FunctionArg arg : arguments) {
      if (arg instanceof Expression) {
        resolved.add(ResolvedArgument.value(((Expression) arg).getType()));
      } else if (arg instanceof Type) {
        resolved.add(ResolvedArgument.type((Type) arg));
      } else if (arg instanceof EnumArg) {
        // An enum argument may carry no option; keep that distinct from an empty one so the
        // identity is faithful and validation can report it against the declaration.
        resolved.add(
            ((EnumArg) arg)
                .value()
                .map(ResolvedArgument::enumOption)
                .orElseGet(ResolvedArgument::unspecifiedEnumOption));
      }
    }
    return resolved;
  }
}
