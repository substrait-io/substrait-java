package io.substrait.expression;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;
import org.immutables.value.Value;

/**
 * Represents an aggregate function invocation, including its declaration, arguments, options,
 * aggregation phase, sort fields, output type, and invocation semantics.
 */
@Value.Immutable
public abstract class AggregateFunctionInvocation {

  /**
   * Returns the aggregate function variant declaration.
   *
   * @return the function variant declaration
   */
  public abstract SimpleExtension.AggregateFunctionVariant declaration();

  /**
   * Returns the ordered list of function arguments.
   *
   * @return list of function arguments
   */
  public abstract List<FunctionArg> arguments();

  /**
   * Returns the options applied to this aggregate function.
   *
   * @return list of function options
   */
  public abstract List<FunctionOption> options();

  /**
   * Returns the aggregation phase (e.g., initial, intermediate, final).
   *
   * @return aggregation phase
   */
  public abstract Expression.AggregationPhase aggregationPhase();

  /**
   * Returns the sort fields applied to this invocation, if any.
   *
   * @return list of sort fields
   */
  public abstract List<Expression.SortField> sort();

  /**
   * Returns the output type produced by this invocation.
   *
   * @return the output type
   */
  public abstract Type outputType();

  /**
   * Returns the type of this invocation (same as {@link #outputType()}).
   *
   * @return the output type
   */
  public Type getType() {
    return outputType();
  }

  /**
   * Returns the aggregation invocation semantics (e.g., aggregate, merge).
   *
   * @return aggregation invocation
   */
  public abstract Expression.AggregationInvocation invocation();

  /**
   * Validates that variadic arguments satisfy the parameter consistency requirement. When
   * CONSISTENT, all variadic arguments must have the same type (ignoring nullability). When
   * INCONSISTENT, arguments can have different types.
   *
   * @throws IllegalArgumentException if validation fails
   */
  @Value.Check
  protected void check() {
    VariadicParameterConsistencyValidator.validate(declaration(), arguments());
  }

  /**
   * Creates a builder for {@link AggregateFunctionInvocation}.
   *
   * @return a new immutable builder
   */
  public static ImmutableAggregateFunctionInvocation.Builder builder() {
    return ImmutableAggregateFunctionInvocation.builder();
  }
}
