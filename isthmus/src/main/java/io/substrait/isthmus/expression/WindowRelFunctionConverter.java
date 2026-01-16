package io.substrait.isthmus.expression;

import static io.substrait.isthmus.expression.WindowBoundConverter.toWindowBound;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.WindowBound;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.AggregateFunctions;
import io.substrait.isthmus.TypeConverter;
import io.substrait.relation.ConsistentPartitionWindow;
import io.substrait.type.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlAggFunction;

/**
 * Converts Calcite window aggregate calls (from {@link Window.RexWinAggCall}) into Substrait {@link
 * ConsistentPartitionWindow.WindowRelFunctionInvocation}s.
 *
 * <p>Handles bounds type (ROWS/RANGE), lower/upper bounds, DISTINCT/ALL invocation, and function
 * signature matching against configured Substrait window function variants.
 */
public class WindowRelFunctionConverter
    extends FunctionConverter<
        SimpleExtension.WindowFunctionVariant,
        ConsistentPartitionWindow.WindowRelFunctionInvocation,
        WindowRelFunctionConverter.WrappedWindowRelCall> {

  /**
   * Returns the supported window function signatures used for matching.
   *
   * @return immutable list of supported signatures.
   */
  @Override
  protected ImmutableList<FunctionMappings.Sig> getSigs() {
    return FunctionMappings.WINDOW_SIGS;
  }

  /**
   * Creates a converter with the provided window function variants.
   *
   * @param functions Supported Substrait window function variants.
   * @param typeFactory Calcite type factory for type handling.
   */
  public WindowRelFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions, RelDataTypeFactory typeFactory) {
    super(functions, typeFactory);
  }

  /**
   * Creates a converter with provided function variants and additional signatures.
   *
   * @param functions Supported Substrait window function variants.
   * @param additionalSignatures Extra signatures to consider during matching.
   * @param typeFactory Calcite type factory for type handling.
   * @param typeConverter Converter for Calcite/Substrait types.
   */
  public WindowRelFunctionConverter(
      List<SimpleExtension.WindowFunctionVariant> functions,
      List<FunctionMappings.Sig> additionalSignatures,
      RelDataTypeFactory typeFactory,
      TypeConverter typeConverter) {
    super(functions, additionalSignatures, typeFactory, typeConverter);
  }

  /**
   * Generates a bound Substrait window relation function invocation for a matched call.
   *
   * @param call Wrapped window rel call, including bounds and the original win-agg call.
   * @param function Selected Substrait function variant.
   * @param arguments Converted Substrait function arguments.
   * @param outputType Result type for the invocation.
   * @return Built {@link ConsistentPartitionWindow.WindowRelFunctionInvocation}.
   */
  @Override
  protected ConsistentPartitionWindow.WindowRelFunctionInvocation generateBinding(
      WrappedWindowRelCall call,
      SimpleExtension.WindowFunctionVariant function,
      List<? extends FunctionArg> arguments,
      Type outputType) {
    Window.RexWinAggCall over = call.getWinAggCall();

    Expression.AggregationInvocation invocation =
        over.distinct
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

    // Calcite only supports ROW or RANGE mode
    Expression.WindowBoundsType boundsType =
        call.isRows() ? Expression.WindowBoundsType.ROWS : Expression.WindowBoundsType.RANGE;
    WindowBound lowerBound = toWindowBound(call.getLowerBound());
    WindowBound upperBound = toWindowBound(call.getUpperBound());

    return ExpressionCreator.windowRelFunction(
        function,
        outputType,
        Expression.AggregationPhase.INITIAL_TO_RESULT,
        invocation,
        boundsType,
        lowerBound,
        upperBound,
        arguments);
  }

  /**
   * Attempts to convert a Calcite {@link Window.RexWinAggCall} into a Substrait window relation
   * function invocation.
   *
   * <p>Resolves the corresponding Substrait aggregate function variant, checks arity using
   * signatures, and builds the invocation if match succeeds.
   *
   * @param winAggCall Calcite window aggregate call.
   * @param lowerBound Lower bound of the window.
   * @param upperBound Upper bound of the window.
   * @param isRows Whether the window uses ROWS (true) or RANGE (false).
   * @param topLevelConverter Function converting top-level {@link RexNode}s to Substrait {@link
   *     Expression}s.
   * @return {@link Optional} containing the {@link
   *     ConsistentPartitionWindow.WindowRelFunctionInvocation} if matched; otherwise empty.
   */
  public Optional<ConsistentPartitionWindow.WindowRelFunctionInvocation> convert(
      Window.RexWinAggCall winAggCall,
      RexWindowBound lowerBound,
      RexWindowBound upperBound,
      boolean isRows,
      Function<RexNode, Expression> topLevelConverter) {
    SqlAggFunction aggFunction = (SqlAggFunction) winAggCall.getOperator();

    SqlAggFunction lookupFunction =
        AggregateFunctions.toSubstraitAggVariant(aggFunction).orElse(aggFunction);
    FunctionFinder m = signatures.get(lookupFunction);
    if (m == null) {
      return Optional.empty();
    }
    if (!m.allowedArgCount(winAggCall.getOperands().size())) {
      return Optional.empty();
    }

    WrappedWindowRelCall wrapped =
        new WrappedWindowRelCall(winAggCall, lowerBound, upperBound, isRows);
    return m.attemptMatch(wrapped, topLevelConverter);
  }

  static class WrappedWindowRelCall implements FunctionConverter.GenericCall {
    private final Window.RexWinAggCall winAggCall;
    private final RexWindowBound lowerBound;
    private final RexWindowBound upperBound;
    private final boolean isRows;

    private WrappedWindowRelCall(
        Window.RexWinAggCall winAggCall,
        RexWindowBound lowerBound,
        RexWindowBound upperBound,
        boolean isRows) {
      this.winAggCall = winAggCall;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
      this.isRows = isRows;
    }

    @Override
    public Stream<RexNode> getOperands() {
      return winAggCall.getOperands().stream();
    }

    @Override
    public RelDataType getType() {
      return winAggCall.getType();
    }

    /**
     * Returns the underlying Calcite window aggregate call.
     *
     * @return the {@link Window.RexWinAggCall}.
     */
    public Window.RexWinAggCall getWinAggCall() {
      return winAggCall;
    }

    /**
     * Returns the lower bound of the window.
     *
     * @return the {@link RexWindowBound} lower bound.
     */
    public RexWindowBound getLowerBound() {
      return lowerBound;
    }

    /**
     * Returns the upper bound of the window.
     *
     * @return the {@link RexWindowBound} upper bound.
     */
    public RexWindowBound getUpperBound() {
      return upperBound;
    }

    /**
     * Whether the window uses ROWS (true) or RANGE (false).
     *
     * @return {@code true} if ROWS; {@code false} if RANGE.
     */
    public boolean isRows() {
      return isRows;
    }
  }
}
