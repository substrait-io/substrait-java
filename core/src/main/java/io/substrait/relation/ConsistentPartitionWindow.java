package io.substrait.relation;

import io.substrait.expression.Expression;
import io.substrait.expression.Expression.SortField;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.FunctionOption;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import io.substrait.util.VisitationContext;
import java.util.List;
import java.util.stream.Stream;
import org.immutables.value.Value;

/**
 * A window relation that ensures consistent partitioning and ordering across all window function
 * invocations in a single input relation. Provides partition expressions, sort keys, and window
 * functions.
 */
@Value.Immutable
@Value.Enclosing
public abstract class ConsistentPartitionWindow extends SingleInputRel implements HasExtension {

  /**
   * Returns the window function invocations applied over the input.
   *
   * @return list of window function invocations
   */
  public abstract List<WindowRelFunctionInvocation> getWindowFunctions();

  /**
   * Returns the expressions used to partition the input rows.
   *
   * @return list of partitioning expressions
   */
  public abstract List<Expression> getPartitionExpressions();

  /**
   * Returns the sort fields defining row order within each partition.
   *
   * @return list of sort fields
   */
  public abstract List<SortField> getSorts();

  /**
   * Derives the output record type by appending window outputs to the input type.
   *
   * @return the resulting struct type
   */
  @Override
  protected Type.Struct deriveRecordType() {
    Type.Struct initial = getInput().getRecordType();
    return TypeCreator.of(initial.nullable())
        .struct(
            Stream.concat(
                initial.fields().stream(),
                getWindowFunctions().stream().map(WindowRelFunctionInvocation::outputType)));
  }

  /**
   * Accepts a relation visitor.
   *
   * @param <O> the result type
   * @param <C> the visitation context type
   * @param <E> the exception type that may be thrown
   * @param visitor the relation visitor
   * @param context the visitation context
   * @return the visit result
   * @throws E if the visitor signals an error
   */
  @Override
  public <O, C extends VisitationContext, E extends Exception> O accept(
      RelVisitor<O, C, E> visitor, C context) throws E {
    return visitor.visit(this, context);
  }

  /**
   * Creates a builder for {@link ConsistentPartitionWindow}.
   *
   * @return a new immutable builder
   */
  public static ImmutableConsistentPartitionWindow.Builder builder() {
    return ImmutableConsistentPartitionWindow.builder();
  }

  /**
   * A single window function invocation with its arguments, options, output type, phase/invocation,
   * and window bounds.
   */
  @Value.Immutable
  public abstract static class WindowRelFunctionInvocation {

    /**
     * Returns the window function variant declaration.
     *
     * @return the function variant declaration
     */
    public abstract SimpleExtension.WindowFunctionVariant declaration();

    /**
     * Returns the ordered list of function arguments.
     *
     * @return list of function arguments
     */
    public abstract List<FunctionArg> arguments();

    /**
     * Returns the options applied to the function invocation.
     *
     * @return list of function options
     */
    public abstract List<FunctionOption> options();

    /**
     * Returns the type produced by this invocation.
     *
     * @return output type of the window function
     */
    public abstract Type outputType();

    /**
     * Returns the aggregation phase (e.g., initial, intermediate, final).
     *
     * @return aggregation phase
     */
    public abstract Expression.AggregationPhase aggregationPhase();

    /**
     * Returns the aggregation invocation semantics (e.g., aggregate, merge).
     *
     * @return aggregation invocation
     */
    public abstract Expression.AggregationInvocation invocation();

    /**
     * Returns the inclusive lower bound of the window frame.
     *
     * @return lower window bound
     */
    public abstract WindowBound lowerBound();

    /**
     * Returns the inclusive upper bound of the window frame.
     *
     * @return upper window bound
     */
    public abstract WindowBound upperBound();

    /**
     * Returns the bounds type (e.g., ROWS or RANGE).
     *
     * @return window bounds type
     */
    public abstract Expression.WindowBoundsType boundsType();

    /**
     * Creates a builder for {@link WindowRelFunctionInvocation}.
     *
     * @return a new immutable builder
     */
    public static ImmutableConsistentPartitionWindow.WindowRelFunctionInvocation.Builder builder() {
      return ImmutableConsistentPartitionWindow.WindowRelFunctionInvocation.builder();
    }
  }
}
