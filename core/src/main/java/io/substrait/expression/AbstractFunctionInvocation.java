package io.substrait.expression;

import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import java.util.List;

/**
 * Represents a generic function invocation, including its declaration, arguments, aggregation
 * phase, sort fields, output type, and invocation details.
 *
 * @param <T> the function type (from {@link SimpleExtension})
 * @param <I> the invocation-specific type
 */
public abstract class AbstractFunctionInvocation<T extends SimpleExtension.Function, I> {

  /**
   * Returns the function declaration associated with this invocation.
   *
   * @return the function declaration
   */
  public abstract T declaration();

  /**
   * Returns the ordered list of function arguments.
   *
   * @return list of function arguments
   */
  public abstract List<FunctionArg> arguments();

  /**
   * Returns the aggregation phase for this invocation, if applicable.
   *
   * @return aggregation phase or {@code null} if not an aggregate
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
   * Returns the invocation-specific details.
   *
   * @return invocation details
   */
  public abstract I invocation();
}
